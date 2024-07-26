import logging

from mock import Mock, mock

from ska_sdp_pipelines.framework.log_util import LogUtil


@mock.patch("ska_sdp_pipelines.framework.log_util.configure_logging")
@mock.patch("ska_sdp_pipelines.framework.log_util.timestamp")
def test_should_configure_with_additional_log_config(
    timestamp_mock, configure_mock
):
    timestamp_mock.return_value = "FORMATTED_TIME"

    pipeline_name = "name"

    expected = {
        "handlers": {
            "file": {
                "()": logging.FileHandler,
                "formatter": "default",
                "filename": "/path/to/output/name_FORMATTED_TIME.log",
            }
        },
        "root": {
            "handlers": ["console", "file"],
        },
    }

    LogUtil.configure(pipeline_name, output_dir="/path/to/output")
    configure_mock.assert_called_once_with(
        level=logging.INFO, overrides=expected
    )


@mock.patch("ska_sdp_pipelines.framework.log_util.configure_logging")
@mock.patch("ska_sdp_pipelines.framework.log_util.timestamp")
def test_should_configure_without_additional_log_config_if_no_output(
    timestamp_mock, configure_mock
):
    timestamp_mock.return_value = "FORMATTED_TIME"

    pipeline_name = "name"

    LogUtil.configure(pipeline_name)
    configure_mock.assert_called_once_with(level=logging.INFO, overrides=None)


@mock.patch("ska_sdp_pipelines.framework.log_util.configure_logging")
@mock.patch("ska_sdp_pipelines.framework.log_util.timestamp")
def test_should_configure_verbose(timestamp_mock, configure_mock):

    timestamp_mock.return_value = "FORMATTED_TIME"

    pipeline_name = "name"

    LogUtil.configure(pipeline_name, verbose=True)
    configure_mock.assert_called_once_with(level=logging.DEBUG, overrides=None)


def test_should_execute_stage_with_logs():
    stage1 = Mock(name="stage1", return_value="stage1 output")
    pipeline_data = {"data": "PIEPLINE_DATA"}

    output = LogUtil.with_log(False, stage1, pipeline_data, kw1=1, kw2=2)

    assert output == "stage1 output"

    stage1.assert_called_once_with(
        pipeline_data,
        kw1=1,
        kw2=2,
    )


@mock.patch("ska_sdp_pipelines.framework.log_util.logging.getLogger")
def test_should_execute_stage_with_logs_as_verbose(get_logger_mock):
    logger_mock = Mock(name="logger")
    get_logger_mock.return_value = logger_mock
    stage1 = Mock(name="stage1", return_value="stage1 output")
    pipeline_data = {"data": "PIEPLINE_DATA"}

    LogUtil.with_log(True, stage1, pipeline_data, kw1=1, kw2=2)

    logger_mock.setLevel.assert_has_calls(
        [mock.call(logging.INFO), mock.call(logging.DEBUG)]
    )
