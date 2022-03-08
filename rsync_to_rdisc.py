#! /usr/bin/env python3
import sys
import os
import datetime
import glob

from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import smtplib
import mimetypes
import socket
from paramiko import SSHClient
from pathlib import Path

import settings


def make_mail(filename, state, reason=None, run_file=None):
    if state == "lost_mount":
        subject = "ERROR: mount lost to BGarray for {}".format(socket.gethostname())
        text = (
            "<html><body><p>Mount to BGarray is lost for {0}</p>"
            "<p>Remove {1} before datatransfer can be restarted</p></body></html>"
        ).format(socket.gethostname(), run_file)
    elif state == "lost_hpc":
        subject = "ERROR: Connection to HPC transfernodes {} are lost".format(filename)
        text = (
            "<html><body><p>Connection to HPC transfernodes {0} are lost</p>"
            "<p>Remove {1} before datatransfer can be restarted</p></body></html>"
        ).format(filename, run_file)
    elif state == "ok":
        subject = "COMPLETED: Transfer to BGarray has succesfully completed for {}".format(filename)
        text = "<html><body><p>Transfer to BGarray has succesfully completed for {}</p></body></html>".format(filename)
    elif state == "error":
        subject = "ERROR: transfer to BGarray has not completed for {}".format(filename)
        text = "<html><body><p>Transfer to BGarray has not been completed for {}</p></body></html>".format(filename)
    elif state == "notcomplete":
        subject = reason
        text = ("<html><body><p> Data not transferred to BGarray. Run {0}</p>"
                "<p>Remove {1} before datatransfer can be restarted</p></body></html>".format(filename, run_file))
    elif state == "settings":
        subject = reason
        text = ("<html><body><p>Settings.py need to be fixed before datatransfer can resume</p>"
                "<p>Remove {} before datatransfer can be restarted</p></body></html>".format(run_file))
    send_email(settings.email_from, settings.email_to, subject, text)


def send_email(sender, receivers, subject, text, attachment=None):
    mail = MIMEMultipart()
    mail['Subject'] = subject
    mail['From'] = sender
    mail['To'] = ';'.join(receivers)
    if attachment:
        filename = attachment.split('/')[-1]
        fp = open(attachment, 'rb')
        ctype, encoding = mimetypes.guess_type(attachment)
        if ctype is None or encoding is not None:
            """ No guess could be made, or the file is encoded (compressed), so use a generic bag-of-bits type."""
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        msg = MIMEBase(maintype, subtype)
        msg.set_payload(fp.read())
        fp.close()
        """Encode the payload using Base64"""
        encoders.encode_base64(msg)
        msg.add_header('Content-Disposition', 'attachment', filename=filename)
        mail.attach(msg)
    msg = MIMEText(text, 'html')
    mail.attach(msg)
    m = smtplib.SMTP("pim.umcutrecht.nl")
    m.sendmail(sender, receivers, mail.as_string())
    m.quit()


def rsync_and_check(action, run, folder, temperror, wkdir, folder_dic, log):
    print(f"Rsync run: {run}")
    os.system(action)
    bgarray_log_file = "{bgarray}/{log}".format(bgarray=settings.bgarray, log=log)
    if int(Path(temperror).stat().st_size) == 0:  # do not include run in transferred_runs.txt if temp error file is not empty.
        transferred_runs_file = "{wkdir}/transferred_runs.txt".format(wkdir=wkdir)
        with open(transferred_runs_file, 'a') as log_file:
            log_file.write("{run}_{folder}\n".format(run=run, folder=folder))

        with open(bgarray_log_file, 'a') as log_file:
            log_file.write("\n>>> No errors detected <<<\n")

        os.system("rm {}".format(temperror))
        print("no errors")
        make_mail("{}{}".format(folder_dic[folder]["input"], run), "ok")
        return "ok"
    else:
        with open(bgarray_log_file, 'a') as log_file:
            log_file.write((
                "\n>>>{run}_{folder} errors detected in Processed data transfer, "
                "not added to completed files <<<\n"
            ).format(run=run, folder=folder))

        os.system(action)
        print("errors, check errorlog file")
        make_mail("{}{}".format(folder_dic[folder]["input"], run), "error")
        return "error"


def check_daemon_running(wkdir):
    run_file = "{}/transfer.running".format(wkdir)
    if os.path.isfile(run_file):
        sys.exit()
    else:
        os.system("touch {}".format(run_file))
        return run_file


def check_mount(bgarray, run_file):
    if os.path.exists(bgarray):
        pass
    else:
        print("Mount is lost.")
        make_mail("mount", "lost_mount", run_file=run_file)
        sys.exit()


def make_dictionary_runs(wkdir):
    transferred_dic = {}
    if os.path.isfile(str(wkdir) + "transferred_runs.txt"):
        with open("{}/transferred_runs.txt".format(wkdir), 'r') as runs:
            for line in runs:
                transferred_dic[line.rstrip()] = ""
    else:
        new_file = open(str(wkdir) + "transferred_runs.txt", "w")
        new_file.close()

    return transferred_dic


def connect_to_remote_server(host_keys, server, user, run_file):
    client = SSHClient()
    client.load_host_keys(host_keys)
    client.load_system_host_keys()
    try:
        client.connect(server[0], username=user)
        hpc_server = server[0]
    except OSError:
        try:
            client.connect(server[1], username=user)
            hpc_server = server[1]
        except OSError:
            make_mail(" and ".join(server), "lost_hpc", run_file=run_file)
            sys.exit("connection to HPC transfernodes are lost")

    return client, hpc_server


def get_folders_remote_server(client, folder_dic):
    to_be_transferred = {}
    for folder in folder_dic:
        stdin, stdout, stderr = client.exec_command("ls {}".format(folder_dic[folder]["input"]))
        folders = stdout.read().decode("utf8")
        for item in folders.split():
            combined = "{0}_{1}".format(item.split()[-1], folder)
            if combined not in transferred_dic:
                to_be_transferred[item.split()[-1]] = folder

    return to_be_transferred


def check_if_file_missing(folder, client, run):
    missing = []
    for check_file in folder["files_required"]:
        if check_file:
            stdin, stdout, stderr = client.exec_command((
                "[[ -f {0}{1}/{2} ]] && echo \"Present\" "
                "|| echo \"Absent\""
            ).format(folder["input"], run, check_file))
            status = stdout.read().decode("utf8").rstrip()
            if status == "Absent":
                missing.append(check_file)
    return missing


def action_if_file_missing(folder, remove_run_file, missing, run):
    if 'continue_without_email' in folder and folder["continue_without_email"]:
        # Do not send a mail and do not lock datatransfer
        pass
    elif 'continue_without_email' in folder and not folder["continue_without_email"]:
        # Send a mail and lock datatransfer
        reason = (
            "Analysis not complete (file(s) {0} missing). "
            "Run = {1} in folder {2} ".format(" and ".join(missing), run, to_be_transferred[run]))
        make_mail(run, "notcomplete", reason, run_file)
        remove_run_file = False
    else:  # Send a mail and lock datatransfer
        reason = ("Unknown status {0} in settings.py for {1}").format(folder["continue_without_email"], folder)
        make_mail(run, "settings", reason, run_file)
        remove_run_file = False

    return remove_run_file


def rsync_server_remote(settings, hpc_server, client, to_be_transferred):
    date = str(datetime.datetime.now()).split(".")[0]
    bgarray_log_file = "{bgarray}/{log}".format(bgarray=settings.bgarray, log=settings.log)
    remove_run_file = True

    temperror = settings.temperror
    folder_dic = settings.folder_dic
    log = settings.log

    for run in to_be_transferred:
        continue_rsync = True
        folder = folder_dic[to_be_transferred[run]]
        action = (
            "rsync -rahuL --stats {user}@{server}:{path}{run} {output}/ "
            " 1>> {bgarray}/{log} 2>> {bgarray}/{errorlog} 2> {temperror}"
        ).format(
            user=settings.user,
            server=hpc_server,
            path=folder["input"],
            run=run,
            output=folder["output"],
            bgarray=settings.bgarray,
            log=log,
            errorlog=settings.errorlog,
            temperror=temperror
        )

        missing = check_if_file_missing(folder, client, run)

        if missing:
            continue_rsync = False
            remove_run_file = action_if_file_missing(folder, remove_run_file, missing, run)

        with open(bgarray_log_file, 'a') as log_file:
            log_file.write("\n#########\nDate: {date}\nRun_folder: {run}\n".format(date=date, run=run))

        if continue_rsync:
            rsync_result = rsync_and_check(action, run, to_be_transferred[run], temperror, settings.wkdir, folder_dic, log)
            if rsync_result == "ok" and folder['upload_gatk_vcf']:
                upload_gatk_vcf(run, "{output}/{run}".format(output=folder["output"], run=run))
            if rsync_result == "ok" and folder['upload_exomedepth_vcf']:
                upload_exomedepth_vcf(run, "{output}/{run}".format(output=folder["output"], run=run))

    return remove_run_file


def upload_gatk_vcf(run, run_folder):
    print(run_folder)
    run = '_'.join(run.split('_')[:4])  # remove projects from run.
    for vcf_file in glob.iglob("{}/single_sample_vcf/*.vcf".format(run_folder)):
        print(f"python vcf_upload.py {vcf_file} VCF_FILE {run}")


def upload_exomedepth_vcf(run, run_folder):
    print(run_folder)
    run = '_'.join(run.split('_')[:4])  # remove projects from run.

    # Parse QC/CNV/220223_A01131_0233_AH775MDMXY_5_exomedepth_summary.txt
    # Look for warnings / DO_NOT_USE_MergeSample

    for vcf_file in glob.iglob("{}/exomedepth/HC/*.vcf".format(run_folder)):
        print(f"python vcf_upload.py {vcf_file} 'UMCU CNV VCF v1' {run}")


if __name__ == "__main__":

    """ If daemon is running exit, else create transfer.running file and continue """
    run_file = check_daemon_running(settings.wkdir)

    """ Check if mount to BGarray intact """
    check_mount(settings.bgarray, run_file)

    """ Make dictionairy of transferred_runs.txt file, or create transferred_runs.txt if not present """
    transferred_dic = make_dictionary_runs(settings.wkdir)

    """ Get folders to be transfer from HPC """
    client, hpc_server = connect_to_remote_server(settings.host_keys, settings.server, settings.user, run_file)
    to_be_transferred = get_folders_remote_server(client, settings.folder_dic)

    """Rsync folders from HPC to bgarray"""
    remove_run_file = rsync_server_remote(settings, hpc_server, client, to_be_transferred)

    """ Remove run_file if transfer daemon shouldn't be blocked to prevent repeated mailing """
    if remove_run_file:
        os.remove(run_file)

    client.close()
