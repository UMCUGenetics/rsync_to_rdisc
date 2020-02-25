#! /usr/bin/env python
import sys, os, re, math, time, commands, datetime
from os.path import join, isdir, isfile, split
from os import system, walk, listdir, chdir, rename, stat,lstat
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import smtplib
import mimetypes
import settings
import datetime, time
from os import stat
from pwd import getpwuid
import socket
####################################################################################################################################
# AUTHOR:       M.G. Elferink
# DATE:         29-04-2014
# Purpose:	Automatic sync Dx-run folders within to /data/DIT-bgarray/
#		Exome, GENEPANEL, and NIPT data
#		
#		This script is excecuted with a cronjob (each hour), or can be manually runned by executing ./sync_to_rdisc.py 
#		However, only for user melferink!	
#
####################################################################################################################################

def find_owner(filename):
    return getpwuid(lstat(filename).st_uid).pw_name

def make_mail(filename,state):
	if state[0] == "ok": 
	        try:
	                email_to=[settings.owner_dic[find_owner(filename)]]
	        except:
     			email_to = settings.finished_mail
		""" Send complete mail """
	        subject = 'COMPLETED: Transfer to BGarray has succesfully completed for {}'.format(filename)
        	text = "<html><body><p>"+'Transfer to BGarray has succesfully completed for {}'.format(filename)+"</p></body></html>"
	elif state[0] == "error": # send error mail to owner of cronjob/mount
                try:
                        email_to=[settings.owner_dic[find_owner(filename)]]
                        email_to += [settings.finished_mail]
                except:
                        email_to = settings.finished_mail  
		subject = 'ERROR: transfer to BGarray has not completed for {}'.format(filename)
                text = "<html><body><p>"+'Transfer to BGarray has not been completed for {}'.format(filename)+"</p></body></html>"
	elif state[0] == "lost":
                email_to=[]
                try:
                    for item in settings.owner_dic:
                        email_to+=[settings.owner_dic[item]]
                except:
                    email_to = settings.finished_mail
                subject = 'ERROR: mount to BGarray for {}'.format(socket.gethostname())
                text = "<html><body><p>"+'Mount to BGarray is lost for {}'.format(socket.gethostname())+"</p></body></html>"
	elif state[0] == "cleanup":
		try:
                        email_to=[settings.owner_dic[find_owner(filename)]]
                except:
                        email_to = settings.finished_mail
                """ Send complete mail """
                subject = 'PLEASE CLEAN-UP YOUR RUN {} !'.format(filename)
                text = "<html><body><p>"+'Data not transferred to BGarray. Run {}'.format(filename)+"</p></body></html>"
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
        		# No guess could be made, or the file is encoded (compressed), so
        		# use a generic bag-of-bits type.
        		ctype = 'application/octet-stream'
		maintype, subtype = ctype.split('/', 1)
		msg = MIMEBase(maintype, subtype)
		msg.set_payload(fp.read())
		fp.close()
		# Encode the payload using Base64
		encoders.encode_base64(msg)
		msg.add_header('Content-Disposition', 'attachment', filename=filename)
		mail.attach(msg)
	msg = MIMEText(text,'html')
	mail.attach(msg)
	m = smtplib.SMTP('smtp-open.umcutrecht.nl')
	m.sendmail(sender, receivers, mail.as_string())
	m.quit()

def check(action,run,processed,run_org,folder): ## perform actual Rsync command, and check if no errors.
	print "Rsync run:" +str(run)
	os.system(action)
	error= commands.getoutput("wc -l "+str(temperror))
        if int(error.split()[0]) == 0: ## check if there are errors in de temporary error file. If so, do not include runid in transferred_runs.txt
		if processed == 1:
	        	os.system("echo " + str(run)+"_"+str(folder)+">> "+str(wkdir)+"transferred_runs.txt")
		os.system("echo \"\n>>> No errors detected <<<\" >>/data/DIT-bgarray/"+str(log))
        	os.system("rm "+str(temperror))
		print "no errors"
                make_mail(str(wkdir)+str(folder)+"/"+str(run), ["ok"])
		return "ok"
 	else:
		action="echo \">>>\""+str(run)+"_"+str(folder)+" \"errors detected in Processed data transfer, not added to completed files <<<\""
                os.system(action + ">>/data/DIT-bgarray/"+ str(log))
		print "errors, check errorlog file"
		make_mail(run, ["error"])
		return "error"


def sync(action1,action2,item,processed,folder): ## check if run has been (succesfully) synced before. If so, skip, else perform sync
	if folder == "RAW_data_MIPS":
		rundir = []
		sequencedir = os.listdir(str(item))
		for dir in sequencedir: ## loop into sequencers
			runs=os.listdir(str(item)+"/"+str(dir))
			for run in runs:
				rundir +=[str(dir)+"/"+str(run)] 
	else:
		rundir = os.listdir(str(item))

	state ="hold"
	run_list=[]
       	for run in rundir:
        	try: ## check runid in dictionary
			dic[str(run)+"_"+str(folder)]
            	except:
			raw_file=''
			if folder == "Exomes" or folder == "Genepanels":
				raw_file=commands.getoutput("find -L "+str(item)+"/"+str(run)+" -maxdepth 1 -iname \"*.raw_variants.vcf\"")
			if folder == "RAW_data_MIPS":
				done_file='{}/{}/{}/{}'.format(wkdir,folder,run,'TransferDone.txt')
				if not isfile(done_file):
					pass
				else:
					action= str(action1)+str(run)+"\""+str(action2)
                                	os.system("echo \"\n#########\nDate: "+str(date)+"\nRun_folder: "+str(run)+ "\" >>/data/DIT-bgarray/" +str(log))
                                	file_org="" ## dummy
                                	state = check(action,run,processed,file_org,folder)
                                	run_list+=[run]
			else:
				if not isfile(raw_file):
					action= str(action1)+str(run)+"\""+str(action2)
					os.system("echo \"\n#########\nDate: "+str(date)+"\nRun_folder: "+str(run)+ "\" >>/data/DIT-bgarray/" +str(log))
					file_org="" ## dummy
					state = check(action,run,processed,file_org,folder)
					run_list+=[run]
				else:
					make_mail(str(wkdir)+str(folder)+"/"+str(run), ["cleanup"])
	return state,run_list

### START ###

# check if mount to BGarray intact. If not, restore
if os.path.exists("/data/DIT-bgarray/Illumina/") == True:
	pass
else:
	print "Mount is lost. Please contact M. Elferink for restore"
	make_mail("mount", ["lost"])	
	sys.exit()

wkdir=settings.wkdir
dic={}
try:
	runs=open(str(wkdir)+"transferred_runs.txt","r").readlines()
	for line in runs:
        	dic[line.rstrip()]=""

except:
	new_file=open(str(wkdir)+"transferred_runs.txt","w")
	new_file.close()

# if running exit, else create transfer.running file and continue
running=str(wkdir)+"/transfer.running"
if os.path.isfile(running):
    sys.exit()
else:
    os.system("touch "+running)


## Rsync folders ##

log =settings.log
errorlog=settings.errorlog
temperror=settings.temperror

date= str(datetime.datetime.now()).split(".")[0]
lib_dir = os.walk(str(wkdir)).next()[1]
for item in lib_dir:
	processed=1
	state=[""] 
	if item== "Exomes":
		folder="Exomes"
		action1="rsync -rahuL --stats --exclude '*fastq.gz' \""+str(wkdir)+"Exomes/"
		action2=" /data/DIT-bgarray/Illumina/Exomes/ 1>>/data/DIT-bgarray/"+str(log)+" 2>>/data/DIT-bgarray/" +str(errorlog)+" 2>"+str(temperror)
		state=sync(action1,action2,str(wkdir)+str(item),processed,folder)

	elif item == "TRANSFER":
		folder="TRANSFER"
		action1="rsync -rahuL --stats \""+ str(wkdir)+"TRANSFER/"
       		action2=" /data/DIT-bgarray/TRANSFER/ 1>>/data/DIT-bgarray/"+str(log)+" 2>>/data/DIT-bgarray/" +str(errorlog)+" 2>"+str(temperror)
 		processed=1
		state=sync(action1,action2,str(wkdir)+str(item),processed,folder)
	
        elif item == "MIPS":
                folder = "MIPS"
                action1="rsync -rahuL --stats \""+ str(wkdir)+"MIPS/"
                action2=" /data/DIT-bgarray/Illumina/MIPS 1>>/data/DIT-bgarray/"+str(log)+" 2>>/data/DIT-bgarray/" +str(errorlog)+" 2>"+str(temperror)
                processed=1
                state=sync(action1,action2,str(wkdir)+str(item),processed,folder)
	
	elif item == "RAW_data_MIPS":
		folder = "RAW_data_MIPS"
                action1="rsync -rahuL --stats \""+ str(wkdir)+"RAW_data_MIPS/"
                action2=" /data/DIT-bgarray/RAW_data/MIPS 1>>/data/DIT-bgarray/"+str(log)+" 2>>/data/DIT-bgarray/" +str(errorlog)+" 2>"+str(temperror)
                processed=1
                state=sync(action1,action2,str(wkdir)+str(item),processed,folder)

	elif item == "RAW_data":
		folder="RAW_data"
                action1="rsync -rahuL --stats \""+ str(wkdir)+"RAW_data/"
                action2=" /data/DIT-bgarray/RAW_data/BACKUP_TEMP/ 1>>/data/DIT-bgarray/"+str(log)+" 2>>/data/DIT-bgarray/" +str(errorlog)+" 2>"+str(temperror)
                processed=1
                state=sync(action1,action2,str(wkdir)+str(item),processed, folder)

        elif item == "Genomes":
                folder = "Genomes"
                action1="rsync -rahuL --stats \""+ str(wkdir)+"Genomes/"
                action2=" /data/DIT-bgarray/Illumina/Genomes 1>>/data/DIT-bgarray/"+str(log)+" 2>>/data/DIT-bgarray/" +str(errorlog)+" 2>"+str(temperror)
                processed=1
                state=sync(action1,action2,str(wkdir)+str(item),processed,folder)

os.system("rm "+running)
