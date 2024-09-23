import logging

from mock import mock

from ska_sdp_piper.piper.utils import LogUtil


@mock.patch("ska_sdp_piper.piper.utils.log_util.configure_logging")
@mock.patch("ska_sdp_piper.piper.utils.log_util.timestamp")
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


@mock.patch("ska_sdp_piper.piper.utils.log_util.configure_logging")
@mock.patch("ska_sdp_piper.piper.utils.log_util.timestamp")
def test_should_configure_without_additional_log_config_if_no_output(
    timestamp_mock, configure_mock
):
    timestamp_mock.return_value = "FORMATTED_TIME"

    pipeline_name = "name"

    LogUtil.configure(pipeline_name)
    configure_mock.assert_called_once_with(level=logging.INFO, overrides=None)


@mock.patch("ska_sdp_piper.piper.utils.log_util.configure_logging")
@mock.patch("ska_sdp_piper.piper.utils.log_util.timestamp")
def test_should_configure_verbose(timestamp_mock, configure_mock):

    timestamp_mock.return_value = "FORMATTED_TIME"

    pipeline_name = "name"

    LogUtil.configure(pipeline_name, verbose=True)
    configure_mock.assert_called_once_with(level=logging.DEBUG, overrides=None)
