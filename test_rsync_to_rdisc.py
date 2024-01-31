#!/usr/bin/env python
from csv import writer
import subprocess
from pathlib import Path

from paramiko import ssh_exception
import pytest
# from socket import timeout

import rsync_to_rdisc


# TODO: split set_up_test
@pytest.fixture(scope="session")
def set_up_test(tmp_path_factory):
    """
    Production folder structure is replicated with fake values.
    There are a few analysis of run 230920_A01131_0356_AHKM7VDRX3.
    1: everything ok.
    2: incomplete, therefore missing several files such as workflow.done
    3: single sample.
    4: multi samples.

    All files required to manage behavior of rsync_to_rdisc are added to tmp_path, unless different location is required.
    """
    tmp_path = tmp_path_factory.mktemp("tmp")

    # Setup settings
    rsync_to_rdisc.settings.wkdir = f"{tmp_path}/wkdir"
    rsync_to_rdisc.settings.temp_error_path = f"{rsync_to_rdisc.settings.wkdir}/temp.error"
    rsync_to_rdisc.settings.log_path = f"{rsync_to_rdisc.settings.wkdir}/Rsync_Dx.log"
    rsync_to_rdisc.settings.errorlog_path = f"{rsync_to_rdisc.settings.wkdir}/Rsync_Dx.errorlog"

    # Setup wkdir files
    Path(rsync_to_rdisc.settings.wkdir).mkdir()
    Path(rsync_to_rdisc.settings.temp_error_path).touch()
    Path(f"{rsync_to_rdisc.settings.wkdir}/temp_not_empty.error").write_text("hello")  # non empty temp error
    Path(rsync_to_rdisc.settings.log_path).touch()
    run_file = f"{rsync_to_rdisc.settings.wkdir}/transfer.running"
    Path(run_file).touch()

    Path(f"{tmp_path}/empty/").mkdir()  # Dir or subfiles have to stay empty.

    # Setup Analysis
    run = "230920_A01131_0356_AHKM7VDRX3"
    analysis = f"{run}_1"  # run ok + exomedepth
    analysis_transfer_settings = rsync_to_rdisc.settings.transfer_settings['bgarray']['transfers'][0]
    Path(f"{rsync_to_rdisc.settings.wkdir}/transferred_runs.txt").write_text(analysis)  # Other analysis will be added as part of the tests.


    # Processed folder
    for project in range(1, 6):
        processed_analysis = Path(tmp_path/f"processed/{run}_{project}")
        for subdir in ["single_sample_vcf", "exomedepth/HC", "QC/CNV/"]:
            Path(tmp_path/f"processed/{run}_{project}/{subdir}").mkdir(parents=True)
        if project != 2:
            Path(processed_analysis/"workflow.done").touch()

    processed_run_dir = Path(tmp_path/f"processed/{run}")

    # Single vcf folders
    empty_vcf_path = Path(tmp_path/f"processed/{run}_2/single_sample_vcf/")
    single_vcf_path = Path(tmp_path/f"processed/{run}_3/single_sample_vcf/")
    multi_vcf_path = Path(tmp_path/f"processed/{run}_4/single_sample_vcf/")
    Path(single_vcf_path/"test.vcf").touch()
    Path(multi_vcf_path/"test_1.vcf").touch()
    Path(multi_vcf_path/"test_2.vcf").touch()

    # CNV / ExomeDepth files
    Path(tmp_path/f"processed/{run}_1/QC/CNV/{run}_1_exomedepth_summary.txt").write_text(
        "U000000CF2023D00000;CM=HC;REFSET=RS-SSv7-2023-4;GENDER=female;CR=0.9900;PD=60.00;TC=95\tWARNING: chr Y in female."
    )
    Path(tmp_path/f"processed/{run}_2/QC/CNV/{run}_2_exomedepth_summary.txt").touch()
    Path(tmp_path/f"processed/{run}_3/QC/CNV/{run}_3_exomedepth_summary.txt").write_text(
        "U000000CF2023D00000;CM=HC;REFSET=RS-SSv7-2023-4;GENDER=female;CR=0.9900;PD=60.00;TC=95"
    )

    multi_sample_summary = Path(tmp_path/f"processed/{run}_4/QC/CNV/{run}_4_exomedepth_summary.txt")

    with open(multi_sample_summary, 'a', newline='\n') as f:
        file_writer = writer(f, delimiter='\t')
        file_writer.writerows([
            ["U000000CF2023D00001;CM=HC;REFSET=RS-SSv7-2023-4;GENDER=female;CR=0.9900;PD=60.00;TC=95"],
            ["U000000CF2023D00002;CM=HC;REFSET=RS-SSv7-2023-4;GENDER=female;CR=0.9900;PD=60.00;TC=95"]
        ])

    # empty_hc_path = Path(tmp_path/f"processed/{run}_2/exomedepth/HC/")
    single_hc_path = Path(tmp_path/f"processed/{run}_3/exomedepth/HC/")
    multi_hc_path = Path(tmp_path/f"processed/{run}_4/exomedepth/HC/")
    Path(single_hc_path/"test_U000000CF2023D00000.vcf").touch()
    Path(multi_hc_path/"test_U000000CF2023D00001.vcf").touch()
    Path(multi_hc_path/"test_U000000CF2023D00002.vcf").touch()

    return {
        "tmp_path": tmp_path, "processed_run_dir": processed_run_dir, "run_file": run_file, "run": run, 'analysis1': analysis,
        "analysis1_transfer_settings": analysis_transfer_settings, "empty_vcf_path": empty_vcf_path,
        "single_vcf_path": single_vcf_path, "multi_vcf_path": multi_vcf_path
    }


@pytest.fixture(scope="function")
def mock_get_upload_state(mocker):
    return mocker.patch("rsync_to_rdisc.get_upload_state")


@pytest.fixture(scope="class")
def mock_path_unlink(class_mocker):
    return class_mocker.patch("rsync_to_rdisc.Path.unlink", autospec=True)


@pytest.fixture(scope="class")
def mock_send_mail_incomplete(class_mocker):
    return class_mocker.patch("rsync_to_rdisc.send_mail_incomplete")


@pytest.fixture(scope="class")
def mock_send_mail_lost_hpc(class_mocker):
    return class_mocker.patch("rsync_to_rdisc.send_mail_lost_hpc")


@pytest.fixture(scope="class")
def mock_send_mail_lost_mount(class_mocker):
    return class_mocker.patch("rsync_to_rdisc.send_mail_lost_mount")


@pytest.fixture(scope="class")
def mock_send_mail_transfer_state(class_mocker):
    return class_mocker.patch("rsync_to_rdisc.send_mail_transfer_state")


@pytest.fixture(scope="class")
def mock_sys_exit(class_mocker):
    return class_mocker.patch("rsync_to_rdisc.sys.exit")


def side_effect_run_vcf_upload(vcf_file, vcf_type, run):
    return [vcf_file]


@pytest.fixture(scope="class")
def mock_run_vcf_upload(class_mocker):
    return class_mocker.patch("rsync_to_rdisc.run_vcf_upload", side_effect=side_effect_run_vcf_upload)


class TestCheckRsync():
    def test_ok(self, set_up_test, mocker):
        rsync_result = rsync_to_rdisc.check_rsync(
            set_up_test['analysis1'], set_up_test['analysis1_transfer_settings']
        )
        assert rsync_result == "ok"
        assert not Path(rsync_to_rdisc.settings.temp_error_path).exists()
        assert "No errors detected" in Path(rsync_to_rdisc.settings.log_path).read_text()

    def test_temperror(self, mock_send_mail_transfer_state, set_up_test):
        rsync_to_rdisc.settings.temp_error_path = f"{rsync_to_rdisc.settings.wkdir}/temp_not_empty.error"
        rsync_result = rsync_to_rdisc.check_rsync(
            set_up_test['analysis1'], set_up_test['analysis1_transfer_settings']
        )
        assert rsync_result == "error"
        assert f"{set_up_test['analysis1']}_Exomes errors detected" in Path(rsync_to_rdisc.settings.log_path).read_text()
        mock_send_mail_transfer_state.assert_called_once()

        # Reset all mocks
        mock_send_mail_transfer_state.reset_mock()


class TestCheckDaemonRunning():
    def test_new_file(self, set_up_test):
        out = rsync_to_rdisc.check_daemon_running(f"{set_up_test['tmp_path']}/empty/")
        assert Path(f"{set_up_test['tmp_path']}/empty/transfer.running").exists()
        assert out == Path(f"{set_up_test['tmp_path']}/empty/transfer.running")

    def test_file_exists(self, set_up_test, mock_sys_exit):
        rsync_to_rdisc.check_daemon_running(rsync_to_rdisc.settings.wkdir)
        mock_sys_exit.assert_called_once()
        mock_sys_exit.reset_mock()


class TestIsMountAvailable():
    def test_mount_exists(self, set_up_test, mock_send_mail_lost_mount):
        assert rsync_to_rdisc.is_mount_available('bgarray', set_up_test['tmp_path'], set_up_test['run_file'])
        mock_send_mail_lost_mount.assert_not_called()

    def test_lost_mount(self, set_up_test, mock_send_mail_lost_mount, mock_sys_exit):
        assert not rsync_to_rdisc.is_mount_available('bgarray', 'fake_path', set_up_test['run_file'])
        mock_send_mail_lost_mount.assert_called_once_with('bgarray', set_up_test['run_file'])
        # Reset mock
        mock_send_mail_lost_mount.reset_mock()
        mock_sys_exit.reset_mock()


class TestGetTransferredRuns():
    def test_get(self, set_up_test):
        transferred_runs = rsync_to_rdisc.get_transferred_runs(rsync_to_rdisc.settings.wkdir)
        assert transferred_runs == {set_up_test['analysis1']}

    def test_empty_transferred_runs(self, set_up_test, mocker):
        mock_touch = mocker.patch("rsync_to_rdisc.Path.touch")
        transferred_runs = rsync_to_rdisc.get_transferred_runs(f"{set_up_test['tmp_path']}/empty/")
        assert not transferred_runs  # Empty set
        mock_touch.assert_called_once_with(Path(f"{set_up_test['tmp_path']}/empty/transferred_runs.txt"))


class TestConnectToRemoteServer():
    def test_connect_ok(self, mocker, set_up_test):
        fake_ssh_client = mocker.MagicMock()
        with mocker.patch("rsync_to_rdisc.SSHClient", return_value=fake_ssh_client):  # TODO: raises warning, should change.
            rsync_to_rdisc.connect_to_remote_server("host_keys", ["hpct04", "hpct05"], "user", set_up_test['run_file'])
        fake_ssh_client.load_host_keys.assert_called_once_with("host_keys")
        fake_ssh_client.load_system_host_keys.assert_called_once()
        # max nr provided servers and not 0
        assert fake_ssh_client.connect.call_count <= 2 and fake_ssh_client.connect.call_count

    def test_raises_OSerror(self, mocker, set_up_test, mock_send_mail_lost_hpc, mock_sys_exit):
        fake_ssh_client = mocker.MagicMock()
        fake_ssh_client.connect.side_effect = OSError
        with mocker.patch("rsync_to_rdisc.SSHClient", return_value=fake_ssh_client):  # TODO: raises warning, should change.
            rsync_to_rdisc.connect_to_remote_server("host_keys", ["hpct04"], "user", set_up_test['run_file'])
        mock_send_mail_lost_hpc.assert_called_once_with("hpct04", set_up_test['run_file'])
        mock_sys_exit.assert_called_once_with("Connection to HPC transfer nodes are lost.")
        # Reset
        mock_send_mail_lost_hpc.reset_mock()
        mock_sys_exit.reset_mock()

    # TODO: test socket.timeout
    @pytest.mark.parametrize("side", [ssh_exception.SSHException, ssh_exception.AuthenticationException])
    def test_raises_errors(self, side, mocker, set_up_test, mock_path_unlink, mock_sys_exit):
        fake_ssh_client = mocker.MagicMock()
        fake_ssh_client.connect.side_effect = side
        with mocker.patch("rsync_to_rdisc.SSHClient", return_value=fake_ssh_client):  # TODO: raises warning, should change.
            rsync_to_rdisc.connect_to_remote_server("host_keys", ["hpct04"], "user", set_up_test['run_file'])
        mock_sys_exit.assert_called_once_with("HPC connection timeout/SSHException/AuthenticationException")
        mock_path_unlink.assert_called_once_with(Path(set_up_test['run_file']))

        # Reset
        mock_path_unlink.reset_mock()
        mock_sys_exit.reset_mock()


class TestGetFoldersRemoteServer():
    def test_ok(self, set_up_test, mocker):
        stdout = mocker.MagicMock()
        stdout.read().decode("utf8").split.return_value = ["analysis1", "analysis2"]
        client = mocker.MagicMock()
        client.exec_command.return_value = "", stdout, ""
        to_transfer = rsync_to_rdisc.get_folders_remote_server(
            client, [{"name": "Exomes", "input": ""}], set_up_test['run_file'], {set_up_test['analysis1']}
        )
        assert to_transfer == {'analysis1': {"name": "Exomes", "input": ""}, 'analysis2': {"name": "Exomes", "input": ""}}

    @pytest.mark.parametrize("side", [ConnectionResetError, TimeoutError])
    def test_errors(self, side, set_up_test, mocker, mock_path_unlink):
        client = mocker.MagicMock()
        client.exec_command.side_effect = side
        with pytest.raises(SystemExit) as system_error:
            rsync_to_rdisc.get_folders_remote_server(
                client, [{"name": "Exomes", "input": ""}], set_up_test['run_file'], {set_up_test['analysis1']}
            )
        mock_path_unlink.assert_called_once_with(Path(set_up_test['run_file']))
        assert system_error.type == SystemExit
        assert str(system_error.value) == "HPC connection ConnectionResetError/TimeoutError"

        # Reset
        mock_path_unlink.reset_mock()


class TestCheckIfFileMissing():
    @pytest.mark.parametrize("return_val,expected", [("Absent", ["workflow.done"]), ("Present", [])])
    def test_file_available(self, return_val, expected, set_up_test, mocker):
        stdout = mocker.MagicMock()
        stdout.read().decode("utf8").rstrip.return_value = return_val
        client = mocker.MagicMock()
        client.exec_command.return_value = "", stdout, ""
        # how to test client.exec_command?
        missing = rsync_to_rdisc.check_if_file_missing(["workflow.done"], f"{set_up_test['processed_run_dir']}_1", client)
        assert missing == expected


class TestActionIfFileMissing():
    def test_without_email(self, set_up_test):
        return_bool = rsync_to_rdisc.action_if_file_missing(
            {"continue_without_email": True}, True, "", set_up_test['analysis1'], set_up_test['run_file']
        )
        assert return_bool

    @pytest.mark.parametrize("folder,template,subject", [
        ({"name": "Exomes", "input": "", "continue_without_email": False}, "transfer_notcomplete", "Analysis not complete"),
        ({"name": "Exomes", "input": ""}, "settings", "Unknown status"),
        ({"name": "Exomes", "input": "", "continue_without_email": "fake"}, "settings", "Unknown status")
    ])
    def test_with_email(self, folder, template, subject, set_up_test, mock_send_mail_incomplete):
        return_bool = rsync_to_rdisc.action_if_file_missing(
            folder, True, "", set_up_test['analysis1'], set_up_test['run_file']
        )
        mock_send_mail_incomplete.assert_called_once()
        assert template == mock_send_mail_incomplete.call_args[0][1]
        assert mock_send_mail_incomplete.call_args[0][2].startswith(subject)
        assert not return_bool
        mock_send_mail_incomplete.reset_mock()


class TestRsyncServerRemote():
    def test_missing_file(self, set_up_test, mocker):
        mock_check = mocker.patch("rsync_to_rdisc.check_if_file_missing", return_value=["workflow.done"])
        mock_action = mocker.patch("rsync_to_rdisc.action_if_file_missing", return_value=False)
        rsync_to_rdisc.rsync_server_remote(
            "hpct04", "client",
            {f"{set_up_test['run']}_2": rsync_to_rdisc.settings.transfer_settings['bgarray']['transfers'][0]},
            set_up_test['tmp_path'], f"{rsync_to_rdisc.settings.wkdir}/transferred_runs.txt",
        )
        mock_check.assert_called_once()
        assert mock_check.call_args[0][0] == ["workflow.done"]
        assert f"{set_up_test['run']}_2" in mock_check.call_args[0][1]
        mock_action.assert_called_once()

    def test_rsync_ok(self, set_up_test, mocker, mock_send_mail_transfer_state):
        mock_check = mocker.patch("rsync_to_rdisc.check_if_file_missing", return_value=[])
        mock_os_system = mocker.patch("rsync_to_rdisc.os.system")
        mock_check_rsync = mocker.patch("rsync_to_rdisc.check_rsync", return_value="ok")
        rsync_to_rdisc.rsync_server_remote(
            "hpct04", "client",
            {f"{set_up_test['run']}_3": rsync_to_rdisc.settings.transfer_settings['bgarray']['transfers'][2]},
            set_up_test['tmp_path'], f"{rsync_to_rdisc.settings.wkdir}/transferred_runs.txt",
        )
        mock_check.assert_called_once()
        mock_os_system.assert_called_once()
        mock_check_rsync.assert_called_once()
        mock_send_mail_transfer_state.assert_called_once()
        mock_send_mail_transfer_state.reset_mock()
        assert f"{set_up_test['run']}_3_TRANSFER\tok" in Path(f"{rsync_to_rdisc.settings.wkdir}/transferred_runs.txt").read_text()

    def test_rsync_error(self, set_up_test, mocker, mock_send_mail_transfer_state):
        mocker.patch("rsync_to_rdisc.check_if_file_missing", return_value=[])
        mocker.patch("rsync_to_rdisc.os.system")
        mocker.patch("rsync_to_rdisc.check_rsync", return_value="error")
        rsync_to_rdisc.rsync_server_remote(
            "hpct04", "client",
            {f"{set_up_test['run']}_3": rsync_to_rdisc.settings.transfer_settings['bgarray']['transfers'][0]},
            set_up_test['tmp_path'], f"{rsync_to_rdisc.settings.wkdir}/transferred_runs.txt",
        )
        assert f"{set_up_test['run']}_3_Exomes" not in Path(f"{rsync_to_rdisc.settings.wkdir}/transferred_runs.txt").read_text()

    # parametrize GATK / ExomeDepth error and no errors.
    @pytest.mark.parametrize("project,gatk_succes,ed_succes,state", [
        ("5", "ok", "error", "vcf_upload_error"),
        ("6", "error", "ok", "vcf_upload_error"),
        ("7", "error", "error", "vcf_upload_error"),
        ("5", "ok", "warning", "vcf_upload_warning"),
        ("6", "warning", "ok", "vcf_upload_warning"),
        ("7", "warning", "warning", "vcf_upload_warning"),
        ("5", "warning", "error", "vcf_upload_error"),
        ("6", "error", "warning", "vcf_upload_error"),
    ])
    def test_vcf_upload_error(self, project, gatk_succes, ed_succes, state, set_up_test, mocker, mock_send_mail_transfer_state):
        analysis = f"{set_up_test['run']}_{project}"
        mocker.patch("rsync_to_rdisc.check_if_file_missing", return_value=[])
        mocker.patch("rsync_to_rdisc.os.system")
        mocker.patch("rsync_to_rdisc.check_rsync", return_value="ok")
        mocker.patch("rsync_to_rdisc.upload_gatk_vcf", return_value=(gatk_succes, ""))
        mocker.patch("rsync_to_rdisc.upload_exomedepth_vcf", return_value=(ed_succes, ""))

        rsync_to_rdisc.rsync_server_remote(
            "hpct04", "client",
            {f"{analysis}": rsync_to_rdisc.settings.transfer_settings['bgarray']['transfers'][0]},
            set_up_test['tmp_path'], f"{rsync_to_rdisc.settings.wkdir}/transferred_runs.txt",
        )
        mock_send_mail_transfer_state.assert_called_once_with(
            filename=f'/hpc/diaggen/data/upload/Exomes/{analysis}',
            state=state,
            upload_result_gatk="",
            upload_result_exomedepth="",
        )
        mock_send_mail_transfer_state.reset_mock()
        assert f"{analysis}_Exomes\t{state}" in Path(f"{rsync_to_rdisc.settings.wkdir}/transferred_runs.txt").read_text()


def test_run_vcf_upload(mocker, set_up_test):
    # Create MagicMock for subprocess.run
    mock_subprocess = mocker.patch.object(subprocess, "run")

    # Simulate the return value of subprocess.run
    stdout = mocker.MagicMock()
    # Set the stdout value according to what the subprocess should return
    stdout.strip().split.return_value = ["passed", "done"]
    # Set the created stdout as return value stdout of the mocked subprocess
    mock_subprocess.return_value.stdout = stdout
    # Simulate a successful execution, not a must to pass this test?
    # mock_subprocess.return_value.returncode = 0

    # Execute the rsync_to_rdisc.run_vcf_upload, this will use the mocked subprocess
    out = rsync_to_rdisc.run_vcf_upload("fake.vcf", 'VCF_FILE', set_up_test['analysis1'])

    # Check if subprocess.run is called once
    mock_subprocess.assert_called_once()

    # For the check the command must be a string,
    # mock_subprocess.call_args[0] is a tuple so join them in an empty string
    command = ''.join(mock_subprocess.call_args[0])

    # Perform other assertions based on your output (out) and subprocess behavior requirements
    required_strs = ["activate", "vcf_upload.py", "fake.vcf", 'VCF_FILE', set_up_test['analysis1']]
    assert all([required_str in command for required_str in required_strs])
    assert out == ["passed", "done"]


@pytest.mark.parametrize("msg,expected", [
    (["error"], "error"),
    (["Error"], "error"),
    (["vcf_upload_error"], "error"),
    (["warning"], "warning"),
    (["Warning"], "warning"),
    (["vcf_upload_warning"], "warning"),
    (["ok"], "ok")
])
def test_get_upload_state(msg, expected):
    return_state = rsync_to_rdisc.get_upload_state(msg)
    assert return_state == expected


class TestUploadGatkVcf():
    @pytest.mark.parametrize("vcf_folder_key,expected", [
        ("empty_vcf_path", 0),
        ("single_vcf_path", 1),
        ("multi_vcf_path", 2),
    ])
    def test_no_vcf(self, set_up_test, vcf_folder_key, expected, mock_run_vcf_upload, mock_get_upload_state):
        vcf_folder = set_up_test[vcf_folder_key]
        run_folder = vcf_folder.parent
        upload_state, upload_result = rsync_to_rdisc.upload_gatk_vcf(run_folder.stem, run_folder)

        assert len(upload_result) == expected  # nr of vcfs uploaded
        assert mock_run_vcf_upload.call_count == expected  # run_vcf_upload is called for each vcf
        mock_get_upload_state.assert_called_once()

        mock_run_vcf_upload.reset_mock()
        mock_get_upload_state.reset_mock()


class TestUploadExomedepthVcf():
    def test_ok(self, set_up_test, mock_run_vcf_upload, mock_get_upload_state):
        upload_state, upload_result = rsync_to_rdisc.upload_exomedepth_vcf(
            f"{set_up_test['run']}_3", f"{set_up_test['processed_run_dir']}_3"
        )

        test_vcf = f"{set_up_test['processed_run_dir']}_3/exomedepth/HC/test_U000000CF2023D00000.vcf"
        mock_run_vcf_upload.assert_called_once_with(test_vcf, 'UMCU CNV VCF v1', set_up_test['run'])
        mock_get_upload_state.assert_called_once_with([test_vcf])

        mock_run_vcf_upload.reset_mock()
        mock_get_upload_state.reset_mock()

    def test_ok_multi(self, set_up_test, mock_run_vcf_upload):
        upload_state, upload_result = rsync_to_rdisc.upload_exomedepth_vcf(
            f"{set_up_test['run']}_4", f"{set_up_test['processed_run_dir']}_4"
        )

        test_vcf_1 = f"{set_up_test['processed_run_dir']}_4/exomedepth/HC/test_U000000CF2023D00001.vcf"
        test_vcf_2 = f"{set_up_test['processed_run_dir']}_4/exomedepth/HC/test_U000000CF2023D00002.vcf"
        assert mock_run_vcf_upload.call_count == 2
        mock_run_vcf_upload.assert_any_call(test_vcf_1, 'UMCU CNV VCF v1', set_up_test['run'])
        mock_run_vcf_upload.assert_any_call(test_vcf_2, 'UMCU CNV VCF v1', set_up_test['run'])
        assert upload_state == "ok"

        mock_run_vcf_upload.reset_mock()

    def test_warning(self, set_up_test):
        upload_state, upload_result = rsync_to_rdisc.upload_exomedepth_vcf(
            f"{set_up_test['run']}_1", f"{set_up_test['processed_run_dir']}_1"
        )
        assert upload_state == "warning"
        assert "not uploaded" in upload_result[0]
        assert "WARNING" in upload_result[0]
