import logging

from mock import mock

from ska_sdp_piper.piper.utils import LogUtil


@mock.patch("ska_sdp_piper.piper.utils.log_util.configure_logging")
def test_should_configure_with_additional_log_config(configure_mock):
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

    filename = "/path/to/output/name_FORMATTED_TIME.log"

    LogUtil.configure(filename)
    configure_mock.assert_called_once_with(
        level=logging.INFO, overrides=expected
    )


@mock.patch("ska_sdp_piper.piper.utils.log_util.configure_logging")
def test_should_configure_without_additional_log_config_if_no_output(
    configure_mock,
):
    LogUtil.configure()
    configure_mock.assert_called_once_with(level=logging.INFO, overrides=None)


@mock.patch("ska_sdp_piper.piper.utils.log_util.configure_logging")
def test_should_configure_verbose(configure_mock):
    LogUtil.configure(verbose=True)
    configure_mock.assert_called_once_with(level=logging.DEBUG, overrides=None)
