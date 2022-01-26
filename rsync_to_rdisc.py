#! /usr/bin/env python3
import sys
import os
import subprocess
import datetime
from pwd import getpwuid
from os.path import join, isfile, split

from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import smtplib
import mimetypes
import socket
from paramiko import SSHClient, AutoAddPolicy

import settings

def find_owner(filename):
    return getpwuid(os.lstat(filename).st_uid).pw_name


def make_mail(filename, state, reason=None, run_file=None):
    if state[0] == "lost":
        email_to = [settings.owner_dic[item] for item in settings.owner_dic]
        subject = "ERROR: mount lost to BGarray for {}".format(socket.gethostname())
        text = "<html><body><p>Mount to BGarray is lost for {}</p></body></html>".format(socket.gethostname())
    elif state[0] == "ok":
        subject = "COMPLETED: Transfer to BGarray has succesfully completed for {}".format(filename)
        text = "<html><body><p>Transfer to BGarray has succesfully completed for {}</p></body></html>".format(filename)
    elif state[0] == "error":
        subject = "ERROR: transfer to BGarray has not completed for {}".format(filename)
        text = "<html><body><p>Transfer to BGarray has not been completed for {}</p></body></html>".format(filename)
    elif state[0] == "notcomplete":
        subject = "Exome analysis not complete (no {0} file). Run = {1}!".format(reason, filename)
        text = "<html><body><p> Data not transferred to BGarray. Run {0}</p><p>Remove {1} before datatransfer can be restarted</p></body></html>".format(filename, run_file)
    elif state[0] == "settings":
        subject = "{}".format(reason)
        text = "<html><body><p>Settings.py need to be fixed before datatransfer can resume!<\p><p>Remove {} before datatransfer can be restarted</p></body></html>".format(run_file)

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


def rsync_and_check (action, run, folder):
    print("Rsync run:{}".format(run))
    os.system(action)
    error = subprocess.getoutput("wc -l {}".format(temperror))
    bgarray_log_file = "{bgarray}/{log}".format(bgarray=settings.bgarray, log=log)
    if int(error.split()[0]) == 0: ## check if there are errors in de temporary error file. If so, do not include runid in transferred_runs.txt
        transferred_runs_file = "{wkdir}/transferred_runs.txt".format(wkdir=wkdir)
        with open(transferred_runs_file, 'a') as log_file:
            log_file.write("{run}_{folder}\n".format(run=run, folder=folder))

        with open(bgarray_log_file, 'a') as log_file:
            log_file.write("\n>>> No errors detected <<<\n")

        os.system("rm {}".format(temperror))
        print("no errors")
        make_mail("{}/{}/{}".format(wkdir, folder, run), ["ok"])
        return "ok"
    else:
        with open(bgarray_log_file, 'a') as log_file:
            log_file.write("\n>>>{run}_{folder} errors detected in Processed data transfer, not added to completed files <<<\n".format(run=run, folder=folder))
        os.system(action)
        print("errors, check errorlog file")
        make_mail("{}/{}/{}".format(wkdir, folder, run), ["error"])
        return "error"


if __name__ == "__main__":

    """ Check if mount to BGarray intact """
    if os.path.exists("{bgarray}".format(bgarray=settings.bgarray)) == True:
        pass
    else:
        print("Mount is lost.")
        make_mail("mount", ["lost"])
        sys.exit()
   

    """ If daemon is running exit, else create transfer.running file and continue """
    wkdir = settings.wkdir
    run_file = "{}/transfer.running".format(wkdir)
    if os.path.isfile(run_file):
        sys.exit()
    else:
        os.system("touch {}".format(run_file))


    """ Make dictionairy of transferred_runs.txt file, or create transferred_runs.txt if not present """
    transferred_dic = {}
    if os.path.isfile(str(wkdir) + "transferred_runs.txt"):
        runs = open(str(wkdir) + "transferred_runs.txt", "r").readlines()
        for line in runs:
            transferred_dic[line.rstrip()] = ""
    else:
        new_file = open(str(wkdir) + "transferred_runs.txt", "w")
        new_file.close()


    """ Get folders to be transfer from HPC """
    client = SSHClient()
    client.load_host_keys(settings.host_keys)
    client.load_system_host_keys()
    client.connect(settings.server, username=settings.user)

    to_be_transferred = {}
    folder_dic = settings.folder_dic
    for folder in folder_dic:
        stdin, stdout, stderr = client.exec_command("ls {}".format(folder_dic[folder][0]))
        folders = stdout.read().decode("utf8")
        for item in folders.split():
            combined = "{0}_{1}".format(item.split()[-1], folder)
            if combined not in transferred_dic:
                to_be_transferred[item.split()[-1]] = folder

    for item in to_be_transferred:
        print("{0} from {1} will be transfer to LABarray".format(item, to_be_transferred[item]))

    """Rsync folders from HPC to bgarray"""
    log = settings.log
    errorlog = settings.errorlog
    temperror = settings.temperror
    date = str(datetime.datetime.now()).split(".")[0]
    bgarray_log_file = "{bgarray}/{log}".format(bgarray=settings.bgarray, log=log)
    remove_run_file = True

    for run in to_be_transferred:
        continue_rsync = True
        state = [""]
        folder =  settings.folder_dic[to_be_transferred[run]]
        action = "rsync -rahuL --stats {host}:{path}{run} {output}/ 1>> {bgarray}/{log} 2>> {bgarray}/{errorlog} 2> {temperror}".format(
            host=settings.remote_server,
            path=folder[0],
            run=run,
            output=folder[1],
            bgarray=settings.bgarray,
            log=log,
            errorlog=errorlog,
            temperror=temperror
        )

        if folder[2]: ## check if certain files need to be present in folder:
            file_path = "{0}{1}/{2}".format(folder[0], run, folder[2])
            status = subprocess.getoutput('ssh -q {0} [[ -f {1} ]] && echo "Present" || echo "Absent"'.format(settings.remote_server, file_path))
            if status == "Absent":
                if folder[3] == "True":  ## Do not send a mail, 
                    continue
                elif folder[3] == "False":  ## Send a mail and lock datatransfer
                    reason = "Exome analysis not complete (no {0} file). Run = {1}".format(folder[2], run)
                    make_mail(run, ["notcomplete"], reason, run_file)
                    continue_rsync = False
                    remove_run_file = False
                else:  ## Send a mail and lock datatransfer
                    reason = "Unknown status {0} in settings.py for {1}".format(folder[3], folder)
                    make_mail(run, ["settings"], reason, run_file)
                    continue_rsync = False
                    remove_run_file = False

        with open(bgarray_log_file, 'a') as log_file:
            log_file.write("\n#########\nDate: {date}\nRun_folder: {run}\n".format(date = date,  run = run))

        if continue_rsync == True:
            rsync_and_check(action, run, to_be_transferred[run])

    if remove_run_file == True:  # only remove run_file is transfer daemon shouldn't be blocked to prevent repeated mailing
        os.system("rm {}".format(run_file))
