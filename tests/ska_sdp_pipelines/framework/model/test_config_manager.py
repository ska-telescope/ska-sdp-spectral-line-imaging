import pytest
from mock import MagicMock, Mock, mock

from ska_sdp_pipelines.framework.exceptions import (
    ConfigNotInitialisedException,
)
from ska_sdp_pipelines.framework.model.config_manager import ConfigManager


@pytest.fixture(autouse=True)
def setup():
    ConfigManager._instance = None


@mock.patch("ska_sdp_pipelines.framework.model.config_manager.yaml")
@mock.patch("builtins.open")
def test_should_instantiate_config(open_mock, yaml_mock):
    file_obj = Mock("file_obj")
    file_obj.write = Mock("readlines")
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = file_obj
    open_mock.return_value = enter_mock

    yaml_mock.safe_load.return_value = {
        "pipeline": {"stage1": True, "stage2": False, "stage3": True}
    }

    ConfigManager.init("/path/to/yaml")

    open_mock.assert_called_once_with("/path/to/yaml", "r")
    yaml_mock.safe_load.assert_called_once_with(file_obj)


def test_should_return_stages_to_run():
    conf = ConfigManager(
        {
            "pipeline": {"stage1": True, "stage2": False, "stage3": True},
            "parameters": {
                "stage1": {"stage1_parameter_1": 0},
                "stage2": {"stage2_parameter_1": 0},
                "stage3": {"stage3_parameter_1": 0},
            },
        }
    )

    expected_stages_to_run = ["stage1", "stage3"]
    assert expected_stages_to_run == conf.stages_to_run


def test_should_return_stages_config():
    conf = ConfigManager(
        {
            "pipeline": {"stage1": True, "stage2": False, "stage3": True},
            "parameters": {
                "stage1": {"stage1_parameter_1": 0},
                "stage2": {"stage2_parameter_1": 0},
                "stage3": {"stage3_parameter_1": 0},
            },
        }
    )

    expected_stage_config = {"stage2_parameter_1": 0}
    assert expected_stage_config == conf.stage_config("stage2")


def test_should_throw_exception_if_config_is_not_initialised():
    with pytest.raises(ConfigNotInitialisedException):
        ConfigManager.get_config()
