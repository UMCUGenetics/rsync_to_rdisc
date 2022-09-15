#! /usr/bin/env python3
import sys
import os
import datetime
import glob
import subprocess

from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import smtplib
import mimetypes
import socket
from paramiko import SSHClient
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

import settings


def make_mail(filename, state, reason=None, run_file=None, upload_result_gatk=None, upload_result_exomedepth=None):
    env = Environment(loader=FileSystemLoader("templates"))

    if state == "lost_mount":
        hostname = socket.gethostname()
        subject = "ERROR: mount lost to BGarray for {}".format(hostname)
        template = env.get_template("lost_mount.html")
        text = template.render(hostname=hostname, run_file=run_file)
    elif state == "lost_hpc":
        subject = "ERROR: Connection to HPC transfernodes {} are lost".format(filename)
        template = env.get_template("lost_hpc.html")
        text = template.render(filename=filename, run_file=run_file)
    elif state == "ok":
        subject = "COMPLETED: Transfer to BGarray has succesfully completed for {}".format(filename)
        template = env.get_template("transfer_ok.html")
        text = template.render(
            filename=filename,
            upload_result_gatk=upload_result_gatk,
            upload_result_exomedepth=upload_result_exomedepth
        )
    elif state == "error":
        subject = "ERROR: transfer to BGarray has not completed for {}".format(filename)
        template = env.get_template("transfer_error.html")
        text = template.render(filename=filename)
    elif state == "notcomplete":
        subject = reason
        template = env.get_template("transfer_notcomplete.html")
        text = template.render(filename=filename, run_file=run_file)
    elif state == "settings":
        subject = reason
        template = env.get_template("settings.html")
        text = template.render(run_file=run_file)
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
    os.system(action)
    bgarray_log_file = "{bgarray}/{log}".format(bgarray=settings.bgarray, log=log)
    if int(Path(temperror).stat().st_size) == 0:  # do not include run in transferred_runs.txt if temp error file is not empty.
        transferred_runs_file = "{wkdir}/transferred_runs.txt".format(wkdir=wkdir)
        with open(transferred_runs_file, 'a') as log_file:
            log_file.write("{run}_{folder}\n".format(run=run, folder=folder))

        with open(bgarray_log_file, 'a') as log_file:
            log_file.write("\n>>> No errors detected <<<\n")

        os.system("rm {}".format(temperror))
        return "ok"
    else:
        with open(bgarray_log_file, 'a') as log_file:
            log_file.write((
                "\n>>>{run}_{folder} errors detected in Processed data transfer, "
                "not added to completed files <<<\n"
            ).format(run=run, folder=folder))

        os.system(action)
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
    except (OSError, socket.timeout):
        try:
            client.connect(server[1], username=user)
            hpc_server = server[1]
        except OSError:
            make_mail(" and ".join(server), "lost_hpc", run_file=run_file)
            sys.exit("connection to HPC transfernodes are lost")
        except socket.timeout:
            sys.exit("HPC connection timeout")
            os.remove(run_file)

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
            if rsync_result == "ok":
                upload_result_gatk = None
                upload_result_exomedepth = None
                if folder['upload_gatk_vcf']:
                    upload_result_gatk = upload_gatk_vcf(
                        run=run,
                        run_folder="{output}/{run}".format(output=folder["output"], run=run)
                    )
                if folder['upload_exomedepth_vcf']:
                    upload_result_exomedepth = upload_exomedepth_vcf(
                        run=run,
                        run_folder="{output}/{run}".format(output=folder["output"], run=run)
                    )
                make_mail(
                    filename="{}{}".format(folder["input"], run),
                    state=rsync_result,
                    upload_result_gatk=upload_result_gatk,
                    upload_result_exomedepth=upload_result_exomedepth
                )

    return remove_run_file


def run_vcf_upload(vcf_file, vcf_type, run):
    upload_vcf = subprocess.run(
            (
                f"source {settings.alissa_vcf_upload}/venv/bin/activate && "
                f"python {settings.alissa_vcf_upload}/vcf_upload.py {vcf_file} '{vcf_type}' {run}"
            ),
            shell=True,
            stdout=subprocess.PIPE,
            encoding='UTF-8'
        )
    # Cleanup upload_vcf output: Strip and split on new line, remove empty strings from list
    upload_vcf_out = list(filter(None, upload_vcf.stdout.strip().split('\n')))
    return upload_vcf_out


def upload_gatk_vcf(run, run_folder):
    run = '_'.join(run.split('_')[:4])  # remove projects from run.
    upload_result = []
    for vcf_file in glob.iglob("{}/single_sample_vcf/*.vcf".format(run_folder)):
        output_vcf_upload = run_vcf_upload(vcf_file, 'VCF_FILE', run)
        if output_vcf_upload:
            upload_result.extend(output_vcf_upload)
    return upload_result


def upload_exomedepth_vcf(run, run_folder):
    # Parse <run>_exomedepth_summary.txt
    cnv_samples = {}
    upload_result = []
    vcf_files = glob.glob("{}/exomedepth/HC/*.vcf".format(run_folder))
    with open(f'{run_folder}/QC/CNV/{run}_exomedepth_summary.txt') as exomedepth_summary:
        for line in exomedepth_summary:
            line = line.strip()
            # Skip empty or comment lines
            if not line or line.startswith('#'):
                continue

            # Parse sample
            if 'WARNING' in line.upper():
                warnings = line.split('\t')[1:]
                sample = line.split(';')[0]
            else:
                warnings = ''
                sample = line.split(';')[0]

            cnv_samples[sample] = '\t'.join(warnings)

    run = '_'.join(run.split('_')[:4])  # remove project from run.
    for sample in cnv_samples:
        if cnv_samples[sample]:
            upload_result.append(f"{sample} not uploaded\t{cnv_samples[sample]}")
        else:
            vcf_file = [vcf for vcf in vcf_files if sample in vcf][0]  # one vcf per sample
            output_vcf_upload = run_vcf_upload(vcf_file, 'UMCU CNV VCF v1', run)
            if output_vcf_upload:
                upload_result.extend(output_vcf_upload)
    return upload_result


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
