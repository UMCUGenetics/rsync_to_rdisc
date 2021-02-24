#! /usr/bin/env python
import sys
import os
import commands
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
import settings

####################################################################################################################################
# Purpose:    Automatic sync Dx-run folders to bgarray
####################################################################################################################################

def find_owner(filename):
    return getpwuid(os.lstat(filename).st_uid).pw_name

def make_mail(filename, state):

    if state[0] is "lost":
        file_owner = ''
        email_to = []
        for item in settings.owner_dic:
            email_to += [settings.owner_dic[item]]
        email_to += settings.finished_mail
    else:
        file_owner = find_owner(filename)
        if file_owner not in settings.owner_dic:
            email_to = [settings.finished_mail]
        else:
            email_to = [settings.owner_dic[file_owner]]

    if state[0] is "ok":
        subject = "COMPLETED: Transfer to BGarray has succesfully completed for {}".format(filename)
        text = "<html><body><p>Transfer to BGarray has succesfully completed for {}</p></body></html>".format(filename)
    elif state[0] is "error":
        subject = "ERROR: transfer to BGarray has not completed for {}".format(filename)
        text = "<html><body><p>Transfer to BGarray has not been completed for {}</p></body></html>".format(filename)
    elif state[0] is "lost":
        subject = "ERROR: mount lost to BGarray for {}".format(socket.gethostname())
        text = "<html><body><p>Mount to BGarray is lost for {}</p></body></html>".format(socket.gethostname())
    elif state[0] is "notcomplete":
        subject = "Exome analysis not complete (no workflow.done file). Run = {}!".format(filename)
        text = "<html><body><p> Data not transferred to BGarray. Run {}</p></body></html>".format(filename)

    if file_owner not in settings.owner_dic and state[0] is not "lost":
        subject = "WARNING user {} does not exist! {}".format(file_owner, subject)

    send_email(settings.email_from, email_to, subject, text)

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
    m = smtplib.SMTP('smtp-open.umcutrecht.nl')
    m.sendmail(sender, receivers, mail.as_string())
    m.quit()

def check(action, run, processed, run_org, folder): ## perform actual Rsync command, and check if no errors.
    print "Rsync run:{}".format(run)
    os.system(action)
    error = commands.getoutput("wc -l {}".format(temperror))
    bgarray_log_file = "{bgarray}/{log}".format(bgarray=settings.bgarray, log=log)
    if int(error.split()[0]) == 0: ## check if there are errors in de temporary error file. If so, do not include runid in transferred_runs.txt
        if processed == 1:
            transferred_runs_file = "{wkdir}/transferred_runs.txt".format(wkdir = wkdir)
            with open(transferred_runs_file, 'a') as log_file:
                log_file.write("{run}_{folder}\n".format(run = run, folder = folder))

        with open(bgarray_log_file, 'a') as log_file:
            log_file.write("\n>>> No errors detected <<<\n")

        os.system("rm {}".format(temperror))
        print "no errors"
        make_mail("{}/{}/{}".format(wkdir, folder, run), ["ok"])
        return "ok"
    else:
        with open(bgarray_log_file, 'a') as log_file:
            log_file.write("\n>>>{run}_{folder} errors detected in Processed data transfer, not added to completed files <<<\n".format(run=run, folder=folder))
        os.system(action)
        print "errors, check errorlog file"
        make_mail("{}/{}/{}".format(wkdir, folder, run), ["error"])
        return "error"

def sync(action1, action2, folder, processed, item): ## check if run has been (succesfully) synced before. If so, skip, else perform sync
    if item == "RAW_data_MIPS":
        rundir = []
        sequencedir = os.listdir(str(folder))
        for sequencefolder in sequencedir: ## loop into sequencers
            runs = os.listdir("{}/{}".format(folder, sequencefolder))
            for run in runs:
                rundir += ["{}/{}".format(sequencefolder, run)]
    else:
        rundir = os.listdir(str(folder))

    state = "hold"
    run_list = []
    for run in rundir:
        analysis = "{}_{}".format(run, item)
        if analysis not in transferred_dic: # Skip run if already in transferred.txt file 
            bgarray_log_file = "{bgarray}/{log}".format(bgarray=settings.bgarray, log = log) 
            if item == "RAW_data_MIPS":
                done_file = '{}/{}/{}/{}'.format(wkdir, item, run, 'TransferDone.txt')
                if not isfile(done_file):
                    pass
                else:
                    action = "{}/{} {}".format(action1,run,action2)
                    with open(bgarray_log_file, 'a') as log_file: 
                        log_file.write("\n#########\nDate: {date}\nRun_folder: {run}".format(date = date,  run = run))
                    file_org = "" ## dummy
                    state = check(action, run, processed, file_org, item)
                    run_list += [run]
            else:
                if item == "Exomes" and not os.path.isfile("{}/{}/workflow.done".format(folder, run)):  ## If exome run is not completed.
                    make_mail("{}/{}/{}".format(wkdir, item, run), ["notcomplete"])
                else:
                    action = "{}/{} {}".format(action1, run, action2)
                    with open(bgarray_log_file, 'a') as log_file:
                        log_file.write("\n#########\nDate: {date}\nRun_folder: {run}".format(date = date,  run = run))
                    file_org = "" ## dummy
                    state = check(action, run, processed, file_org, item)
                    run_list += [run]
    return state, run_list

"""Check if mount to BGarray intact. If not, restore."""
if os.path.exists("{bgarray}/Illumina/".format(bgarray=settings.bgarray)) == True:
    pass
else:
    print "Mount is lost. Please contact M. Elferink for restore"
    make_mail("mount", ["lost"])    
    sys.exit()

wkdir = settings.wkdir
transferred_dic = {}

if os.path.isfile(str(wkdir) + "transferred_runs.txt"):
    runs = open(str(wkdir) + "transferred_runs.txt", "r").readlines()
    for line in runs:
        transferred_dic[line.rstrip()] = ""
else:
    new_file = open(str(wkdir) + "transferred_runs.txt", "w")
    new_file.close()

"""If running exit, else create transfer.running file and continue"""
running = str(wkdir) + "/transfer.running"
if os.path.isfile(running):
    sys.exit()
else:
    os.system("touch " + running)

"""Rsync folders."""
log = settings.log
errorlog = settings.errorlog
temperror = settings.temperror
date = str(datetime.datetime.now()).split(".")[0]
lib_dir = os.walk(str(wkdir)).next()[1]

for item in lib_dir:
    processed = 1
    state = [""]
    if item not in settings.folder_dic:
        pass
    else:
        folder = "{}/{}".format(wkdir, item)
        action1 = "rsync -rahuL --stats {}".format(folder)
        action2 = " {output}/ 1>> {bgarray}/{log} 2>> {bgarray}/{errorlog} 2> {temperror}".format(
            output = settings.folder_dic[item],
            bgarray=settings.bgarray,
            log = log,
            errorlog = errorlog,
            temperror = temperror
            ) 
        state = sync(action1, action2, folder, processed, item)

os.system("rm {}".format(running))
