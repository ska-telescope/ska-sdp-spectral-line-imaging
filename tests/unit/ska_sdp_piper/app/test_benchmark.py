import pytest
from mock import MagicMock, mock

from ska_sdp_piper.app.benchmark import (
    DOOL_BIN,
    DOOL_URL,
    MODULE_DIR,
    run_dool,
    setup_dool,
)


@mock.patch("ska_sdp_piper.app.benchmark.urlretrieve")
@mock.patch("ska_sdp_piper.app.benchmark.tarfile")
@mock.patch("ska_sdp_piper.app.benchmark.os")
@mock.patch("ska_sdp_piper.app.benchmark.Path")
def test_setup_dool_success(
    mock_path, mock_os, mock_tarfile, mock_urlretrieve
):
    mock_tarfile.open.return_value = mock_tarfile
    tarobject = MagicMock(name="tarobject")
    mock_tarfile.__enter__.return_value = tarobject

    mock_path.return_value.exists.return_value = True

    setup_dool()

    mock_urlretrieve.assert_called_once_with(DOOL_URL, filename="dool.tar.gz")
    mock_tarfile.open.assert_called_once_with("dool.tar.gz", "r:gz")
    tarobject.extractall.assert_called_once_with(path=MODULE_DIR)
    mock_path.assert_called_once_with("dool.tar.gz")
    mock_os.remove.assert_called_once_with("dool.tar.gz")


@mock.patch("ska_sdp_piper.app.benchmark.urlretrieve")
@mock.patch("ska_sdp_piper.app.benchmark.tarfile")
@mock.patch("ska_sdp_piper.app.benchmark.os")
@mock.patch("ska_sdp_piper.app.benchmark.Path")
def test_setup_dool_failure(
    mock_path, mock_os, mock_tarfile, mock_urlretrieve
):
    mock_urlretrieve.side_effect = Exception("Download failed")
    mock_path.return_value.exists.return_value = False

    with pytest.raises(
        Exception, match="Error while setting up dool: Download failed"
    ):
        setup_dool()

    mock_path.assert_called_once_with("dool.tar.gz")
    mock_os.remove.assert_not_called()


@pytest.fixture(scope="function")
def timestamp_mock():
    with mock.patch(
        "ska_sdp_piper.app.benchmark.timestamp"
    ) as timestamp_mocked:
        timestamp_mocked.return_value = "timestamp"
        yield timestamp_mocked


@mock.patch("ska_sdp_piper.app.benchmark.subprocess")
def test_run_dool_success(mock_subprocess, timestamp_mock):
    dool_process = MagicMock(name="dool process")
    dool_process.pil = 1234
    dool_process.poll.return_value = None
    mock_subprocess.Popen.return_value = dool_process

    command = ["abc", "def"]

    run_dool("output_dir", "prefix", command, 100)

    mock_subprocess.Popen.assert_called_once_with(
        [
            DOOL_BIN,
            "--time",
            "--mem",
            "--swap",
            "--io",
            "--aio",
            "--disk",
            "--fs",
            "--net",
            "--cpu",
            "--cpu-use",
            "--output",
            "output_dir/prefix_timestamp.csv",
            "100",
        ]
    )
    mock_subprocess.run.assert_called_once_with(["abc", "def"], check=True)
    dool_process.kill.assert_called_once_with()


@mock.patch("ska_sdp_piper.app.benchmark.subprocess")
def test_should_raise_exception_for_run_command_failure(
    mock_subprocess, timestamp_mock
):
    output_dir = "output_dir"
    file_prefix = "test"
    command = ["invalid_command"]

    mock_subprocess.Popen.return_value.poll.return_value = None

    mock_subprocess.run.side_effect = Exception("Command failed")

    with pytest.raises(
        Exception, match="Error during rundool execution: Command failed"
    ):
        run_dool(str(output_dir), file_prefix, command)

    mock_subprocess.Popen.return_value.kill.assert_called_once_with()


@mock.patch("ska_sdp_piper.app.benchmark.subprocess")
def test_should_raise_exception_for_dool_popen_failure(
    mock_subprocess, timestamp_mock
):
    output_dir = "output_dir"
    file_prefix = "test"
    command = ["command"]

    mock_subprocess.Popen.side_effect = Exception("Dool failed")

    with pytest.raises(
        Exception, match="Error during rundool execution: Dool failed"
    ):
        run_dool(str(output_dir), file_prefix, command)

    mock_subprocess.Popen.return_value.kill.assert_not_called()
