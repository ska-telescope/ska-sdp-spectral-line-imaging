import logging

from mock import Mock, mock

from ska_sdp_pipelines.framework.log_util import additional_log_config


@mock.patch("ska_sdp_pipelines.framework.log_util.datetime")
def test_should_return_additional_log_config(datetime_mock):
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

    actual = additional_log_config(pipeline_name)

    assert actual == expected
