import logging

from mock import Mock, mock

from ska_sdp_pipelines.framework.log_util import LogUtil


@mock.patch("ska_sdp_pipelines.framework.log_util.configure_logging")
@mock.patch("ska_sdp_pipelines.framework.log_util.datetime")
def test_should_configure_with_additional_log_config(
    datetime_mock, configure_mock
):
    now_mock_object = Mock(name="now")
    now_mock_object.strftime.return_value = "FORMATTED_TIME"
    datetime_mock.now.return_value = now_mock_object

    pipeline_name = "name"

    expected = {
        "handlers": {
            "file": {
                "()": logging.FileHandler,
                "formatter": "default",
                "filename": "name_FORMATTED_TIME.log",
            }
        },
        "root": {
            "handlers": ["console", "file"],
        },
    }

    LogUtil.configure(pipeline_name)
    configure_mock.assert_called_once_with(
        level=logging.INFO, overrides=expected
    )


@mock.patch("ska_sdp_pipelines.framework.log_util.configure_logging")
@mock.patch("ska_sdp_pipelines.framework.log_util.datetime")
def test_should_configure_verbose(datetime_mock, configure_mock):
    now_mock_object = Mock(name="now")
    now_mock_object.strftime.return_value = "FORMATTED_TIME"
    datetime_mock.now.return_value = now_mock_object

    pipeline_name = "name"

    expected = {
        "handlers": {
            "file": {
                "()": logging.FileHandler,
                "formatter": "default",
                "filename": "name_FORMATTED_TIME.log",
            }
        },
        "root": {
            "handlers": ["console", "file"],
        },
    }

    LogUtil.configure(pipeline_name, verbose=True)
    configure_mock.assert_called_once_with(
        level=logging.DEBUG, overrides=expected
    )


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
