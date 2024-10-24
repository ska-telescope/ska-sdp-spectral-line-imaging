import pytest
from mock import MagicMock, Mock, mock

from ska_sdp_piper.piper.utils.io_utils import (
    create_output_dir,
    read_yml,
    timestamp,
    write_yml,
)


@pytest.fixture(scope="function")
def timestamp_mock():
    with mock.patch(
        "ska_sdp_piper.piper.utils.io_utils.timestamp"
    ) as timestamp_mocked:
        timestamp_mocked.return_value = "timestamp"
        yield timestamp_mocked


@mock.patch("ska_sdp_piper.piper.utils.io_utils.os.makedirs")
@mock.patch("ska_sdp_piper.piper.utils.io_utils.os.path.exists")
def test_should_create_root_output_folder_and_timestamped_folder(
    exists_mock, makedirs_mock, timestamp_mock
):
    exists_mock.return_value = False

    outfile = create_output_dir("./output", "pipeline_name")
    exists_mock.assert_called_once_with("./output")
    makedirs_mock.assert_has_calls(
        [
            mock.call("./output"),
            mock.call("./output/pipeline_name_timestamp"),
        ]
    )

    assert outfile == "./output/pipeline_name_timestamp"


@mock.patch("ska_sdp_piper.piper.utils.io_utils.os.makedirs")
@mock.patch("ska_sdp_piper.piper.utils.io_utils.os.path.exists")
def test_should_create_only_timestamped_folder(
    exists_mock, makedirs_mock, timestamp_mock
):
    exists_mock.return_value = True

    outfile = create_output_dir("./output", "pipeline_name")
    exists_mock.assert_called_once_with("./output")
    makedirs_mock.assert_called_once_with("./output/pipeline_name_timestamp")

    assert outfile == "./output/pipeline_name_timestamp"


@mock.patch("ska_sdp_piper.piper.utils.io_utils.yaml")
@mock.patch("builtins.open")
def test_should_write_yaml_to_the_given_path(open_mock, yaml_mock):
    output_path = "./output.yml"
    config = {"key": "value"}
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = "opened_obj"
    open_mock.return_value = enter_mock

    write_yml(output_path, config)

    open_mock.assert_called_once_with(output_path, "w")
    yaml_mock.dump.assert_called_once_with(config, "opened_obj")


@mock.patch("ska_sdp_piper.piper.utils.io_utils.yaml")
@mock.patch("builtins.open")
def test_should_read_yaml_file(open_mock, yaml_mock):
    input_path = "./input.yml"

    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = "opened_obj"
    open_mock.return_value = enter_mock

    read_yml(input_path)

    open_mock.assert_called_once_with(input_path, "r")
    yaml_mock.safe_load.assert_called_once_with("opened_obj")


def test_should_generate_timestamp():
    with mock.patch(
        "ska_sdp_piper.piper.utils.io_utils.datetime"
    ) as datetime_mocked:
        now_mock = Mock(name="now")
        datetime_mocked.now.return_value = now_mock
        now_mock.strftime = Mock(
            name="strftime",
            side_effect=["timestamp_1", "timestamp_2", "timestamp_3"],
        )

        assert timestamp(1) == "timestamp_1"
        assert timestamp(2) == "timestamp_2"
        assert timestamp(3) == "timestamp_3"
        assert timestamp(3) == "timestamp_3"
        assert timestamp(1) == "timestamp_1"
