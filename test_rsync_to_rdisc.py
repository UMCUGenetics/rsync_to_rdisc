#!/usr/bin/env python
from pathlib import Path

from paramiko import ssh_exception
import pytest
# from socket import timeout

import rsync_to_rdisc


@pytest.fixture
def set_up_test(tmp_path):
    # setup folder structure in tmpdir
    run = "230920_A01131_0356_AHKM7VDRX3_1"
    processed_run_dir = Path(tmp_path/f"processed/{run}")
    processed_run_dir.mkdir(parents=True)
    Path(tmp_path/"empty.error").touch()
    Path(tmp_path/"tmp.error").write_text("hello")
    Path(tmp_path/"bgarray.log").touch()
    run_file = tmp_path/"transfer.running"
    Path(run_file).touch()
    Path(tmp_path/"transferred_runs.txt").write_text(run)
    Path(tmp_path/"empty/").mkdir()
    Path(processed_run_dir/"workflow.done").touch()

    # single vcf folders
    empty_vcf_path = Path(tmp_path/"processed/230920_A01131_0356_AHKM7VDRX3_2/single_sample_vcf/")
    single_vcf_path = Path(tmp_path/"processed/230920_A01131_0356_AHKM7VDRX3_3/single_sample_vcf/")
    multi_vcf_path = Path(tmp_path/"processed/230920_A01131_0356_AHKM7VDRX3_4/single_sample_vcf/")
    empty_vcf_path.mkdir(parents=True)
    single_vcf_path.mkdir(parents=True)
    multi_vcf_path.mkdir(parents=True)
    single_vcf_path.touch()
    multi_vcf_path.touch()
    multi_vcf_path.touch()

    return {"tmp_path": tmp_path, "processed_run_dir": processed_run_dir, "run_file": run_file, "run": run,
            "empty_vcf_path": empty_vcf_path, "single_vcf_path": single_vcf_path, "multi_vcf_path": multi_vcf_path}


@pytest.fixture(scope="class")
def mock_path_unlink(class_mocker):
    return class_mocker.patch("rsync_to_rdisc.Path.unlink")


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


class TestCheckRsync():
    def test_ok(self, set_up_test, mocker):
        bgarray_log = Path(set_up_test['tmp_path']/"bgarray.log")
        tmperror_empty = Path(set_up_test['tmp_path']/"empty.error")
        rsync_result = rsync_to_rdisc.check_rsync(
            set_up_test['run'], "Exomes", tmperror_empty, bgarray_log
        )
        assert rsync_result == "ok"
        assert not tmperror_empty.exists()
        assert "No errors detected" in bgarray_log.read_text()

        # assert log message

    def test_temperror(self, mock_send_mail_transfer_state, set_up_test):
        bgarray_log = Path(f"{set_up_test['tmp_path']}/bgarray.log")
        rsync_result = rsync_to_rdisc.check_rsync(
            set_up_test['run'], "Exomes", Path(set_up_test['tmp_path']/"tmp.error"), bgarray_log
        )
        assert rsync_result == "error"
        assert f"{set_up_test['run']}_Exomes errors detected" in bgarray_log.read_text()
        mock_send_mail_transfer_state.assert_called_once()

        # Reset all mocks
        mock_send_mail_transfer_state.reset_mock()


class TestCheckDaemonRunning():
    def test_new_file(self, set_up_test):
        rsync_to_rdisc.check_daemon_running(f"{set_up_test['tmp_path']}/empty/")
        assert Path(f"{set_up_test['tmp_path']}/empty/transfer.running").exists()

    def test_file_exists(self, set_up_test, mock_sys_exit):
        rsync_to_rdisc.check_daemon_running(set_up_test['tmp_path'])
        mock_sys_exit.assert_called_once()
        mock_sys_exit.reset_mock()


class TestCheckMount():
    def test_mount_exists(self, set_up_test, mock_send_mail_lost_mount):
        rsync_to_rdisc.check_mount(set_up_test['tmp_path'], set_up_test['run_file'])
        mock_send_mail_lost_mount.assert_not_called()

    def test_lost_mount(self, set_up_test, mock_send_mail_lost_mount, mock_sys_exit):
        rsync_to_rdisc.check_mount("fake_path", set_up_test['run_file'])
        mock_send_mail_lost_mount.assert_called_once()
        mock_sys_exit.assert_called_once()
        # Reset mock
        mock_send_mail_lost_mount.reset_mock()
        mock_sys_exit.reset_mock()


class TestGetTransferredRuns():
    def test_get(self, set_up_test):
        transferred_runs = rsync_to_rdisc.get_transferred_runs(set_up_test['tmp_path'])
        assert transferred_runs == {set_up_test['run']}

    def test_empty_transferred_runs(self, set_up_test, mocker):
        mock_touch = mocker.patch("rsync_to_rdisc.Path.touch")
        transferred_runs = rsync_to_rdisc.get_transferred_runs(f"{set_up_test['tmp_path']}/empty/")
        assert not transferred_runs  # Empty set
        mock_touch.assert_called_once_with(Path(f"{set_up_test['tmp_path']}/empty/transferred_runs.txt"))


class TestConnectToRemoteServer():
    def test_connect_ok(self, mocker, set_up_test):
        fake_ssh_client = mocker.MagicMock()
        with mocker.patch("rsync_to_rdisc.SSHClient", return_value=fake_ssh_client):
            rsync_to_rdisc.connect_to_remote_server("host_keys", ["hpct04", "hpct05"], "user", set_up_test['run_file'])
        fake_ssh_client.load_host_keys.assert_called_once_with("host_keys")
        fake_ssh_client.load_system_host_keys.assert_called_once()
        # max nr provided servers and not 0
        assert fake_ssh_client.connect.call_count <= 2 and fake_ssh_client.connect.call_count

    def test_raises_OSerror(self, mocker, set_up_test, mock_send_mail_lost_hpc, mock_sys_exit):
        fake_ssh_client = mocker.MagicMock()
        fake_ssh_client.connect.side_effect = OSError
        with mocker.patch("rsync_to_rdisc.SSHClient", return_value=fake_ssh_client):
            rsync_to_rdisc.connect_to_remote_server("host_keys", ["hpct04"], "user", set_up_test['run_file'])
        mock_send_mail_lost_hpc.assert_called_once_with("hpct04", set_up_test['run_file'])
        mock_sys_exit.assert_called_once_with("Connection to HPC transfernodes are lost.")
        # Reset
        mock_send_mail_lost_hpc.reset_mock()
        mock_sys_exit.reset_mock()

    # TODO: test socket.timeout
    @pytest.mark.parametrize("side", [ssh_exception.SSHException, ssh_exception.AuthenticationException])
    def test_raises_errors(self, side, mocker, set_up_test, mock_path_unlink, mock_sys_exit):
        fake_ssh_client = mocker.MagicMock()
        fake_ssh_client.connect.side_effect = side
        with mocker.patch("rsync_to_rdisc.SSHClient", return_value=fake_ssh_client):
            rsync_to_rdisc.connect_to_remote_server("host_keys", ["hpct04"], "user", set_up_test['run_file'])
        mock_sys_exit.assert_called_once_with("HPC connection timeout/SSHException/AuthenticationException")
        mock_path_unlink.assert_called_with(set_up_test['run_file'])

        # Reset
        mock_path_unlink.reset_mock()
        mock_sys_exit.reset_mock()


class TestGetFoldersRemoteServer():
    # TODO: finish test_ok
    def test_ok(self, set_up_test, mocker):
        stdout = mocker.MagicMock()
        stdout.read().decode("utf8").split.return_value = ["1", "2"]
        client = mocker.MagicMock()
        client.exec_command.return_value = "", stdout, ""
        rsync_to_rdisc.get_folders_remote_server(
            client, {"Exomes": {"input": ""}}, set_up_test['run_file'], {set_up_test['run']}
        )

    @pytest.mark.parametrize("side", [ConnectionResetError, TimeoutError])
    def test_errors(self, side, set_up_test, mocker, mock_path_unlink):
        client = mocker.MagicMock()
        client.exec_command.side_effect = side
        with pytest.raises(SystemExit) as system_error:
            rsync_to_rdisc.get_folders_remote_server(
                client, {"Exomes": {"input": ""}}, set_up_test['run_file'], {set_up_test['run']}
            )
        mock_path_unlink.assert_called_with(set_up_test['run_file'])
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
        missing = rsync_to_rdisc.check_if_file_missing(["workflow.done"], set_up_test['processed_run_dir'], client)
        assert missing == expected


class TestActionIfFileMissing():
    def test_without_email(self, set_up_test):
        return_bool = rsync_to_rdisc.action_if_file_missing(
            {"continue_without_email": True}, True, "", set_up_test["run"], "Exomes", set_up_test["run_file"]
        )
        assert return_bool

    @pytest.mark.parametrize("folder,template,subject", [
        ({"continue_without_email": False}, "transfer_notcomplete", "Analysis not complete"),
        ({}, "settings", "Unknown status"),
        ({"continue_without_email": "fake"}, "settings", "Unknown status")
    ])
    def test_with_email(self, folder, template, subject, set_up_test, mock_send_mail_incomplete):
        return_bool = rsync_to_rdisc.action_if_file_missing(
            folder, True, "", set_up_test["run"], "Exomes", set_up_test["run_file"]
        )
        mock_send_mail_incomplete.assert_called_once()
        assert template == mock_send_mail_incomplete.call_args[0][1]
        assert mock_send_mail_incomplete.call_args[0][2].startswith(subject)
        assert not return_bool
        mock_send_mail_incomplete.reset_mock()


class TestRsyncServerRemote():
    def test_missing_file(self):
        pass

    def test_rsync_ok(self):
        pass

    def test_rsync_error(self):
        pass

    # parametrize gatk/ed error and no errors.
    def test_vcf_upload(self):
        pass


def test_run_vcf_upload(mocker, set_up_test):
    stdout = mocker.MagicMock()
    stdout.strip().split('\n').return_value = ["", "passed", "done"]
    mock_subprocess = mocker.MagicMock()
    # subprocess.run.return_value = "", stdout, ""
    
    with mocker.patch("rsync_to_rdisc.subprocess", return_value=mock_subprocess):
        out = rsync_to_rdisc.run_vcf_upload("fake.vcf", 'VCF_FILE', set_up_test["run"])

    print(mock_subprocess.call_args_list)
    mock_subprocess.run.assert_called_once()
    command = mock_subprocess.run.call_args[0][1]
    required_strs = ["activate", "vcf_upload.py", "fake.vcf", 'VCF_FILE', set_up_test["run"]]
    assert all([required_str in command for required_str in required_strs])
    assert out == ["passed", "done"]


@pytest.mark.parametrize("msg,expected", [
    (["error"], False),
    (["Error"], False),
    (["vcf_upload_error"], False),
    (["ok"], True)
   ])
def test_check_if_upload_succesful(msg, expected):
    return_bool = rsync_to_rdisc.check_if_upload_successful(msg)
    assert return_bool == expected


class TestUploadGatkVcf():
    def side_effect_run_vcf_upload(self, value):
        return value

    @pytest.mark.parametrize("vcf_folder_key,expected", [
        ("empty_vcf_path", 0),
        ("single_vcf_path", 1),
        ("multi_vcf_path", 2),
    ])
    def test_no_vcf(self, set_up_test, vcf_folder_key, expected, mocker):
        vcf_folder = set_up_test[vcf_folder_key]
        mock_run_vcf_upload = mocker.patch("rsync_to_rdisc.run_vcf_upload", side_effect=self.side_effect_run_vcf_upload)
        mock_check = mocker.patch("rsync_to_rdisc.check_if_upload_successful")
        run_folder = vcf_folder.parents[1]
        upload_successful, upload_result = rsync_to_rdisc.upload_gatk_vcf(run_folder.stem, run_folder)
        assert len(upload_result) == expected
        assert mock_run_vcf_upload.call_count == expected
        mock_check.assert_called_once()


class TestUploadExomedepthVcf():
    pass
