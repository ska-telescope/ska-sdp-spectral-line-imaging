from mock import Mock, mock

from ska_sdp_pipelines.framework.io_utils import (
    create_output_name,
    read_dataset,
)


@mock.patch("ska_sdp_pipelines.framework.io_utils.os.makedirs")
@mock.patch("ska_sdp_pipelines.framework.io_utils.datetime")
@mock.patch("ska_sdp_pipelines.framework.io_utils.Path")
def test_should_create_output_name(path_mock, datetime_mock, makedirs_mock):
    now_mock = Mock("now")
    datetime_mock.now.return_value = now_mock
    now_mock.strftime = Mock("strftime", return_value="timestamp")
    path_mock.return_value = path_mock
    path_mock.parent.absolute.return_value = "/absolute/path"
    outfile = create_output_name("infile_path", "pipeline_name", create=True)
    makedirs_mock.assert_called_once_with("/absolute/path/output")
    assert outfile == "/absolute/path/output/pipeline_name_out_timestamp"


@mock.patch(
    "ska_sdp_pipelines.framework.io_utils.read_processing_set",
    return_value="PROCESSING_SET",
)
def test_should_read_given_dataset(read_processing_set_mock):
    infile = "./path/to/infile"
    ps = read_dataset(infile)
    read_processing_set_mock.assert_called_once_with(ps_store=infile)

    assert ps == "PROCESSING_SET"
