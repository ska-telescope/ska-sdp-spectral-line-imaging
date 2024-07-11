from mock import Mock, mock

from ska_sdp_pipelines.framework.io_utils import (
    create_output_dir,
    read_dataset,
)


@mock.patch("ska_sdp_pipelines.framework.io_utils.os.makedirs")
@mock.patch("ska_sdp_pipelines.framework.io_utils.os.path.exists")
@mock.patch("ska_sdp_pipelines.framework.io_utils.datetime")
def test_should_create_root_output_folder_and_timestamped_folder(
    datetime_mock, exists_mock, makedirs_mock
):
    now_mock = Mock(name="now")
    datetime_mock.now.return_value = now_mock
    now_mock.strftime = Mock(name="strftime", return_value="timestamp")
    exists_mock.return_value = False

    outfile = create_output_dir("./output", "pipeline_name")
    exists_mock.assert_called_once_with("./output")
    makedirs_mock.assert_has_calls(
        [
            mock.call("./output"),
            mock.call("./output/pipeline_name_out_timestamp"),
        ]
    )

    assert outfile == "./output/pipeline_name_out_timestamp"


@mock.patch("ska_sdp_pipelines.framework.io_utils.os.makedirs")
@mock.patch("ska_sdp_pipelines.framework.io_utils.os.path.exists")
@mock.patch("ska_sdp_pipelines.framework.io_utils.datetime")
def test_should_create_only_timestamped_folder(
    datetime_mock, exists_mock, makedirs_mock
):
    now_mock = Mock(name="now")
    datetime_mock.now.return_value = now_mock
    now_mock.strftime = Mock(name="strftime", return_value="timestamp")
    exists_mock.return_value = True

    outfile = create_output_dir("./output", "pipeline_name")
    exists_mock.assert_called_once_with("./output")
    makedirs_mock.assert_called_once_with(
        "./output/pipeline_name_out_timestamp"
    )

    assert outfile == "./output/pipeline_name_out_timestamp"


@mock.patch(
    "ska_sdp_pipelines.framework.io_utils.read_processing_set",
    return_value="PROCESSING_SET",
)
def test_should_read_given_dataset(read_processing_set_mock):
    infile = "./path/to/infile"
    ps = read_dataset(infile)
    read_processing_set_mock.assert_called_once_with(ps_store=infile)

    assert ps == "PROCESSING_SET"
