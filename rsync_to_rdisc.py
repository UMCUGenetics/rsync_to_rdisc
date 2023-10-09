#! /usr/bin/env python3
from csv import writer
import glob
import subprocess

from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import smtplib
import mimetypes
import socket
from paramiko import SSHClient, ssh_exception
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
    elif state == "ok" or state == "vcf_upload_error":
        if state == "ok":
            subject = "COMPLETED: Transfer to BGarray has succesfully completed for {}".format(filename)
        elif state == "vcf_upload_error":
            subject = "ERROR: Transfer to BGarray has completed with VCF upload error for {}".format(filename)
        template = env.get_template("transfer_ok.html")
        text = template.render(
            filename=filename,
            upload_result_gatk=upload_result_gatk,
            upload_result_exomedepth=upload_result_exomedepth
        )
    elif state == "error":


def check_rsync(run, folder, temperror, log):
    if not Path(temperror).stat().st_size:
        msg_bgarray_log = [[""], [">>> No errors detected <<<"]]
        Path.unlink(temperror)  # remove tmperror file.
        rsync_result = "ok"
    else:
        msg_bgarray_log = [
            [""], [f">>>{run}_{folder} errors detected in Processed data transfer, not added to completed files <<<"]
        ]
        send_mail_transfer_state("{}{}".format(settings.folder_dic[folder]["input"], run), "error")
        rsync_result = "error"
    with open(log, 'a', newline='\n') as log_file:
        log_file_writer = writer(log_file, delimiter='\t')
        log_file_writer.writerows(msg_bgarray_log)
    return rsync_result


def check_daemon_running(wkdir):
    try:
        Path(f"{wkdir}/transfer.running").touch(exist_ok=False)
    except FileExistsError:
        sys.exit()


def check_mount(bgarray, run_file):
    if not Path(bgarray).exists():
        send_mail_lost_mount("mount", "lost_mount", run_file=run_file)
        sys.exit()


def get_transferred_runs(wkdir):
    transferred_runs = Path(f"{wkdir}/transferred_runs.txt")
    if transferred_runs.is_file():
        with open(transferred_runs, 'r') as runs:
            transferred_set = set(runs.read().splitlines())
        return transferred_set
    else:
        Path.touch(transferred_runs)
        return {}


def connect_to_remote_server(host_keys, server, user, run_file):
    client = SSHClient()
    client.load_host_keys(host_keys)
    client.load_system_host_keys()
    try:
        client.connect(server[0], username=user)
        hpc_server = server[0]
    except (OSError, socket.timeout, ssh_exception.SSHException, ssh_exception.AuthenticationException):
        try:
            client.connect(server[1], username=user)
            hpc_server = server[1]
        except OSError:
            make_mail(" and ".join(server), "lost_hpc", run_file=run_file)
            sys.exit("connection to HPC transfernodes are lost")
        except (socket.timeout, ssh_exception.SSHException, ssh_exception.AuthenticationException):
            os.remove(run_file)
            sys.exit("HPC connection timeout/SSHException/AuthenticationException")

    return client, hpc_server


def get_folders_remote_server(client, folder_dic, run_file):
    to_be_transferred = {}
    for folder in folder_dic:
        try:
            stdin, stdout, stderr = client.exec_command("ls {}".format(folder_dic[folder]["input"]))
        except (ConnectionResetError, TimeoutError):
            os.remove(run_file)
            sys.exit("HPC connection ConnectionResetError/TimeoutError")

        folders = stdout.read().decode("utf8")
        for item in folders.split():
            combined = "{0}_{1}".format(item.split()[-1], folder)
            if combined not in transferred_dic:
                to_be_transferred[item.split()[-1]] = folder

    return to_be_transferred


def check_if_file_missing(required_files, input_folder, client):
    missing = []
    for check_file in required_files:
        if check_file:
            stdin, stdout, stderr = client.exec_command((
                "[[ -f {0}/{1} ]] && echo \"Present\" "
                "|| echo \"Absent\""
            ).format(input_folder, check_file))
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
                email_state = rsync_result

                if folder['upload_gatk_vcf']:
                    upload_successful, upload_result_gatk = upload_gatk_vcf(
                        run=run,
                        run_folder="{output}/{run}".format(output=folder["output"], run=run)
                    )
                    if not upload_successful:
                        email_state = "vcf_upload_error"

                if folder['upload_exomedepth_vcf']:
                    upload_successful, upload_result_exomedepth = upload_exomedepth_vcf(
                        run=run,
                        run_folder="{output}/{run}".format(output=folder["output"], run=run)
                    )
                    if not upload_successful:
                        email_state = "vcf_upload_error"

                make_mail(
                    filename="{}{}".format(folder["input"], run),
                    state=email_state,
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


def check_if_upload_successful(upload_result):
    for msg in upload_result:
        if 'error' in msg.lower():
            return False
    return True


def upload_gatk_vcf(run, run_folder):
    run = '_'.join(run.split('_')[:4])  # remove projects from run.
    upload_result = []

    for vcf_file in glob.iglob("{}/single_sample_vcf/*.vcf".format(run_folder)):
        output_vcf_upload = run_vcf_upload(vcf_file, 'VCF_FILE', run)
        if output_vcf_upload:
            upload_result.extend(output_vcf_upload)

    upload_successful = check_if_upload_successful(upload_result)

    return upload_successful, upload_result


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

    upload_successful = check_if_upload_successful(upload_result)

    return upload_successful, upload_result


if __name__ == "__main__":

    """If daemon is running exit, else create transfer.running file and continue."""
    run_file = check_daemon_running(settings.wkdir)

    """Check if mount to BGarray intact."""
    check_mount(settings.bgarray, run_file)

    """Make set of transferred_runs.txt file, or create transferred_runs.txt if not present."""
    transferred_set = get_transferred_runs(settings.wkdir)

    """Get folders to be transfer from HPC."""
    client, hpc_server = connect_to_remote_server(settings.host_keys, settings.server, settings.user, run_file)
    to_be_transferred = get_folders_remote_server(client, settings.folder_dic, run_file)

    """Rsync folders from HPC to bgarray"""
    remove_run_file = rsync_server_remote(settings, hpc_server, client, to_be_transferred)

    """Remove run_file if transfer daemon shouldn't be blocked to prevent repeated mailing."""
    if remove_run_file:
        os.remove(run_file)

    client.close()
