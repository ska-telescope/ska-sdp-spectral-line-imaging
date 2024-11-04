import logging

from mock import mock

from ska_sdp_piper.piper.utils.log_util import LogPlugin, LogUtil


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


@mock.patch("ska_sdp_piper.piper.utils.log_util.LogUtil")
def test_setup_worker_plugins(log_util_mock):
    log_plugin = LogPlugin(log_file="path/to/log_file", verbose=True)

    log_plugin.setup("mock_worker")
    log_util_mock.configure.assert_called_once_with("path/to/log_file", True)
