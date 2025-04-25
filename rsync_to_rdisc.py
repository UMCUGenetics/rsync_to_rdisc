#! /usr/bin/env python3
from csv import writer
from datetime import datetime
import glob
import subprocess
import sys

from socket import gethostname, timeout
from paramiko import SSHClient, ssh_exception
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from redmail import EmailSender

import settings

# TODO: add docstrings to all functions.

def send_email(subject, template, body_params, attachments=None):
    email = EmailSender(host=settings.email_smtp_host, port=settings.email_smtp_port, use_starttls=False)

    # Setup template environment
    jinja_env = Environment(loader=FileSystemLoader("templates"))
    email.templates_html = jinja_env

    # Send the email
    email.send(
        subject=subject,
        sender=settings.email_from,
        receivers=settings.email_to,
        attachments=attachments,
        html_template=template,
        body_params=body_params,
    )


def send_mail_lost_mount(mount_name, run_file):
    hostname = gethostname()
    send_email(
        subject=f"ERROR: mount lost to {mount_name} for {hostname}",
        template="lost_mount.html",
        body_params={"mount_name": mount_name, "hostname": hostname, "run_file": run_file},
    )


def send_mail_lost_hpc(hpc_host, run_file):
    send_email(
        subject=f"ERROR: Connection to HPC transfernodes {hpc_host} are lost",
        template="lost_hpc.html",
        body_params={"filename": hpc_host, "run_file": run_file},
    )


def send_mail_transfer_state(filename, state, upload_result_gatk=None, upload_result_exomedepth=None):
    body_params = {"filename": filename}
    if state in ["ok", "vcf_upload_error", "vcf_upload_warning"]:
        if state == "ok":
            subject = f"COMPLETED: Transfer has successfully completed for {filename}"
        elif state == "vcf_upload_error":
            subject = f"ERROR: Transfer has completed with VCF upload error for {filename}"
        elif state == "vcf_upload_warning":
            subject = f"COMPLETED: Transfer has completed with VCF upload warning for {filename}"
        template = "transfer_ok.html"
        body_params.update({"upload_result_gatk": upload_result_gatk, "upload_result_exomedepth": upload_result_exomedepth})
    elif state == "error":
        subject = f"ERROR: Transfer has not completed for {filename}"
        template = "transfer_error.html"
    send_email(subject, template, body_params)


def send_mail_incomplete(run, title_template, subject, run_file):
    body_params = {"run_file": run_file}
    if title_template == "transfer_notcomplete":
        body_params["filename"] = run
    send_email(subject, f"{title_template}.html", body_params)


def check_rsync(run, input, ngs_type_name, subprocess_out):
    if not subprocess_out.stderr or not subprocess_out.returncode:
        log_msg = [[""], [">>> No errors detected <<<"]]
        Path(settings.temp_error_path).unlink()  # remove tmperror file.
        rsync_result = "ok"
    else:
        log_msg = [[""], [f">>>{run}_{ngs_type_name} errors detected in data transfer, not added to completed files <<<"]]
        send_mail_transfer_state(f"{input}{run}", "error")
        rsync_result = "error"

    with open(settings.log_path, "a", newline="\n") as log_file:
        log_file_writer = writer(log_file, delimiter="\t")
        log_file_writer.writerows(log_msg)

    return rsync_result


def check_daemon_running(wkdir):
    run_file = Path(f"{wkdir}/transfer.running")
    try:
        run_file.touch(exist_ok=False)
    except FileExistsError:
        sys.exit()
    else:
        return run_file


def is_mount_available(mount_name, mount_path, run_file):
    is_available = True
    try:
        Path(mount_path).exists()
    except (OSError, BlockingIOError):
        is_available = False
    else:
        if not Path(mount_path).exists():
            is_available = False

    # Send email if mount is not available
    if not is_available:
        send_mail_lost_mount(mount_name, run_file)
    return is_available


def get_transferred_runs(wkdir):
    transferred_runs = Path(f"{wkdir}/transferred_runs.txt")
    if transferred_runs.is_file():
        with open(transferred_runs, "r") as runs:
            transferred_set = set()
            for transferred_run_state in set(runs.read().splitlines()):
                # Remove state
                transferred_set.add(transferred_run_state.split("\t")[0])
        return transferred_set
    else:
        Path.touch(transferred_runs)
        return {}


def connect_to_remote_server(host_keys, servers, user, run_file):
    client = SSHClient()
    client.load_host_keys(host_keys)
    client.load_system_host_keys()
    for hpc_server in servers:
        try:
            client.connect(hpc_server, username=user)
            break
        except OSError:
            if hpc_server == servers[-1]:
                send_mail_lost_hpc(" and ".join(servers), run_file)
                sys.exit("Connection to HPC transfer nodes are lost.")
        except (timeout, ssh_exception.SSHException, ssh_exception.AuthenticationException):
            if hpc_server == servers[-1]:
                Path(run_file).unlink()
                sys.exit("HPC connection timeout/SSHException/AuthenticationException")
    return client, hpc_server


def get_folders_remote_server(client, transfers, run_file, transferred_set):
    to_be_transferred = {}
    for transfer in transfers:
        try:
            stdin, stdout, stderr = client.exec_command("ls {}".format(transfer["input"]))
        except (ConnectionResetError, TimeoutError):
            Path(run_file).unlink()
            sys.exit("HPC connection ConnectionResetError/TimeoutError")

        input_folders = stdout.read().decode("utf8").split()
        for input_folder in input_folders:
            combined = f"{input_folder}_{transfer['name']}"
            if combined not in transferred_set:
                to_be_transferred[input_folder] = transfer

    return to_be_transferred


def check_if_file_missing(required_files, input_folder, client):
    missing = []
    for check_file in required_files:
        if check_file:
            stdin, stdout, stderr = client.exec_command(
                ('[[ -f {0}/{1} ]] && echo "Present" || echo "Absent"').format(input_folder, check_file)
            )
            status = stdout.read().decode("utf8").rstrip()
            if status == "Absent":
                missing.append(check_file)
    return missing


def action_if_file_missing(transfer_settings, rsync_succes, missing, run, run_file):
    # Send a mail and lock datatransfer
    if not isinstance(transfer_settings.get("continue_without_email", None), bool):
        reason = "Unknown status {0}: {1} in settings.py for {2}".format(
            "continue_without_email", transfer_settings.get("continue_without_email", None), transfer_settings["name"]
        )
        send_mail_incomplete(run, "settings", reason, run_file)
        return False
    # Do not send a mail and do not lock datatransfer
    elif "continue_without_email" in transfer_settings and transfer_settings["continue_without_email"]:
        return rsync_succes
    # Send a mail and lock datatransfer
    elif "continue_without_email" in transfer_settings and not transfer_settings["continue_without_email"]:
        reason = "Analysis not complete (file(s) {0} missing). Run = {1} in folder {2} ".format(
            " and ".join(missing), run, transfer_settings["input"]
        )
        send_mail_incomplete(run, "transfer_notcomplete", reason, run_file)
        return False


def rsync_server_remote(hpc_server, client, to_be_transferred, mount_path, run_file):
    # TODO: refactor this massive function into more bitesize functions.
    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rsync_succes = True

    for run in to_be_transferred:
        with open(settings.log_path, "a", newline="\n") as log_file:
            log_file_writer = writer(log_file, delimiter="\t")
            log_file_writer.writerows([["#########"], [f"Date: {date}"], [f"Run_folder: {run}"]])

        transfer_settings = to_be_transferred[run]
        # Settings per folder data type, such as remote input dir and local output dir, etc.
        missing = check_if_file_missing(transfer_settings["files_required"], f"{transfer_settings['input']}/{run}", client)

        if missing:
            print(transfer_settings, rsync_succes, missing, run, run_file)
            rsync_succes = action_if_file_missing(transfer_settings, rsync_succes, missing, run, run_file)
            # Don't transfer the run if a required file is missing.
            continue

        # Get include and exclude patterns as list to easily access key-value pair in unit test.
        if transfer_settings.get("include", None):
            include_patterns = [f"--include={pattern}" for pattern in transfer_settings["include"]]
        else:
            include_patterns = []
        if transfer_settings.get("exclude", None):
            exclude_patterns = [f"--exclude={pattern}" for pattern in transfer_settings["exclude"]]
        else:
            exclude_patterns = []

        source_destination = f"{settings.user}@{hpc_server}:{transfer_settings['input']}/{run}"
        target_path = f"{mount_path}/{transfer_settings['output']}"

        rsync_cmd = [
            "rsync",
            "-rahuL",
            "--stats",
            "--prune-empty-dirs",
            *include_patterns,
            *exclude_patterns,
            source_destination,
            target_path,
        ]
        subprocess_result = subprocess.run(rsync_cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, encoding="UTF-8")

        with open(settings.log_path, "a", newline="\n") as log_file:
            log_file.writelines(subprocess_result.stderr)

        # Write stderr to two files
        with open(settings.errorlog_path, "a", newline="\n") as stderr_log:
            stderr_log.writelines(subprocess_result.stderr)

        with open(settings.temp_error_path, "a", newline="\n") as tmp_stderr_file:
            tmp_stderr_file.writelines(subprocess_result.stderr)


        # Check on return code of subprocess.run in check_rsync
        rsync_result = check_rsync(
            run=run,
            input=transfer_settings.get("input"),
            ngs_type_name=transfer_settings.get("name"),
            subprocess_out=subprocess_result,
        )

        if rsync_result == "ok":
            upload_result_gatk = None
            upload_result_exomedepth = None
            email_state = rsync_result

            if transfer_settings["upload_gatk_vcf"]:
                upload_state, upload_result_gatk = upload_gatk_vcf(
                    run=run, run_folder="{output}/{run}".format(output=transfer_settings["output"], run=run)
                )
                if upload_state != "ok":
                    # Warning or error
                    email_state = f"vcf_upload_{upload_state}"

            if transfer_settings["upload_exomedepth_vcf"]:
                upload_state, upload_result_exomedepth = upload_exomedepth_vcf(
                    run=run, run_folder="{output}/{run}".format(output=transfer_settings["output"], run=run)
                )
                # To avoid email_state 'vcf_upload_error' to become a 'vcf_upload_warning'
                if upload_state != "ok" and email_state != "vcf_upload_error":
                    email_state = f"vcf_upload_{upload_state}"

            send_mail_transfer_state(
                filename="{}{}".format(transfer_settings["input"], run),
                state=email_state,
                upload_result_gatk=upload_result_gatk,
                upload_result_exomedepth=upload_result_exomedepth,
            )
            # Do not include run in transferred_runs.txt if temp error file is not empty.
            with open(f"{settings.wkdir}/transferred_runs.txt", "a", newline="\n") as transferred_file:
                file_writer = writer(transferred_file, delimiter="\t")
                file_writer.writerow([f"{run}_{transfer_settings['name']}", email_state])

    return rsync_succes


def run_vcf_upload(vcf_file, vcf_type, run):
    upload_vcf = subprocess.run(
        (
            f"source {settings.alissa_vcf_upload}/venv/bin/activate && "
            f"python {settings.alissa_vcf_upload}/vcf_upload.py {vcf_file} '{vcf_type}' {run}"
        ),
        shell=True,
        stdout=subprocess.PIPE,
        encoding="UTF-8",
    )
    # Cleanup upload_vcf output: Strip and split on new line, remove empty strings from list
    upload_vcf_out = list(filter(None, upload_vcf.stdout.strip().split("\n")))
    return upload_vcf_out


def get_upload_state(upload_result):
    return_value = "ok"
    for msg in upload_result:
        if "error" in msg.lower():
            return_value = "error"
            break
        elif "warning" in msg.lower():
            return_value = "warning"
    return return_value


def upload_gatk_vcf(run, run_folder):
    # Remove projects from run
    run = "_".join(run.split("_")[:4])
    upload_result = []

    for vcf_file in glob.iglob("{}/single_sample_vcf/*.vcf".format(run_folder)):
        output_vcf_upload = run_vcf_upload(vcf_file, "VCF_FILE", run)
        if output_vcf_upload:
            upload_result.extend(output_vcf_upload)
    # Possible states: error, warning or ok.
    upload_state = get_upload_state(upload_result)

    return upload_state, upload_result


def upload_exomedepth_vcf(run, run_folder):
    # Parse <run>_exomedepth_summary.txt
    cnv_samples = {}
    upload_result = []
    vcf_files = glob.glob("{}/exomedepth/HC/*.vcf".format(run_folder))
    with open(f"{run_folder}/QC/CNV/{run}_exomedepth_summary.txt") as exomedepth_summary:
        for line in exomedepth_summary:
            line = line.strip()
            # Skip empty or comment lines
            if not line or line.startswith("#"):
                continue

            # Parse sample
            if "WARNING" in line.upper():
                warnings = line.split("\t")[1:]
                sample = line.split(";")[0]
            else:
                warnings = ""
                sample = line.split(";")[0]

            cnv_samples[sample] = "\t".join(warnings)

    # Remove project from run.
    run = "_".join(run.split("_")[:4])
    for sample in cnv_samples:
        if cnv_samples[sample]:
            upload_result.append(f"{sample} not uploaded\t{cnv_samples[sample]}")
        else:
            vcf_file = [vcf for vcf in vcf_files if sample in vcf][0]  # One vcf per sample
            output_vcf_upload = run_vcf_upload(vcf_file, "UMCU CNV VCF v1", run)
            if output_vcf_upload:
                upload_result.extend(output_vcf_upload)

    # Possible states: error, warning or ok.
    upload_state = get_upload_state(upload_result)

    return upload_state, upload_result


if __name__ == "__main__":
    # If daemon is running exit, else create transfer.running file and continue.
    run_file = check_daemon_running(settings.wkdir)
    remove_run_file = True

    # Make set of transferred_runs.txt file, or create transferred_runs.txt if not present.
    transferred_set = get_transferred_runs(settings.wkdir)

    # Connect to hpc
    client, hpc_server = connect_to_remote_server(settings.host_keys, settings.server, settings.user, run_file)

    # Run rsync commands for each mount point.
    for mount_name in settings.transfer_settings:
        mount_path = settings.transfer_settings[mount_name]["mount_path"]
        # Check if mount is available and continue
        if is_mount_available(mount_name, mount_path, run_file):
            # Get folders to be transferred
            to_be_transferred = get_folders_remote_server(
                client, settings.transfer_settings[mount_name]["transfers"], run_file, transferred_set
            )

            # Rsync folders from HPC to mount
            rsync_succes = rsync_server_remote(hpc_server, client, to_be_transferred, mount_path, run_file)
            if not rsync_succes:
                remove_run_file = False
        else:  # Mount not available block upcoming transfers
            # TODO: Do we want this?
            remove_run_file = False

    # Remove run_file if transfer daemon shouldn't be blocked to prevent repeated mailing.
    if remove_run_file:
        Path(run_file).unlink()

    client.close()
