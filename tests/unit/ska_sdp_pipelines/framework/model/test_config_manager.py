import pytest
from mock import MagicMock, mock

from ska_sdp_pipelines.framework.model.config_manager import ConfigManager


@pytest.fixture(autouse=True)
def setup():
    pipeline = {"stage1": True, "stage2": False, "stage3": True}
    parameters = {
        "stage1": {"stage1_parameter_1": 0},
        "stage2": {"stage2_parameter_1": 0},
        "stage3": {"stage3_parameter_1": 0},
    }
    global_parameters = {
        "global_param_1": 1,
        "global_param_2": 1,
    }
    yield ConfigManager(pipeline, parameters, global_parameters)


def test_should_instantiate_config(setup):
    pipeline = {"stage1": True, "stage2": False, "stage3": True}
    parameters = {
        "stage1": {"stage1_parameter_1": 0},
        "stage2": {"stage2_parameter_1": 0},
        "stage3": {"stage3_parameter_1": 0},
    }
    global_parameters = {
        "global_param_1": 1,
        "global_param_2": 1,
    }
    config_manager = setup

    assert config_manager.pipeline == pipeline
    assert config_manager.parameters == parameters
    assert config_manager.global_parameters == global_parameters


def test_should_return_the_default_config(setup):
    pipeline = {"stage1": True, "stage2": False, "stage3": True}
    parameters = {
        "stage1": {"stage1_parameter_1": 0},
        "stage2": {"stage2_parameter_1": 0},
        "stage3": {"stage3_parameter_1": 0},
    }
    global_parameters = {
        "global_param_1": 1,
        "global_param_2": 1,
    }
    default_config = setup.config

    assert default_config == {
        "parameters": parameters,
        "pipeline": pipeline,
        "global_parameters": global_parameters,
    }


@mock.patch("ska_sdp_pipelines.framework.model.config_manager.yaml")
@mock.patch("builtins.open")
def test_should_update_the_default_config_with_yaml(
    open_mock, yaml_mock, setup
):
    pipeline = {"stage1": False, "stage2": True, "stage3": True}
    parameters = {
        "stage1": {"stage1_parameter_1": 0},
        "stage2": {"stage2_parameter_1": 0},
        "stage3": {"stage3_parameter_1": 10},
    }
    global_parameters = {
        "global_param_1": 20,
        "global_param_2": 25,
    }

    yaml_config = {
        "parameters": parameters,
        "pipeline": pipeline,
        "global_parameters": global_parameters,
    }

    yaml_data = yaml_mock.dump(yaml_config)
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = yaml_data
    yaml_mock.safe_load.return_value = yaml_config
    open_mock.return_value = enter_mock
    config_manager = setup

    config_manager.update_config("/path/to/yaml")

    open_mock.assert_called_once_with("/path/to/yaml", "r")
    yaml_mock.safe_load.assert_called_once_with(yaml_data)

    assert config_manager.pipeline == pipeline
    assert config_manager.parameters == parameters
    assert config_manager.global_parameters == global_parameters


def test_should_update_the_config_with_selected_stages_from_cli(setup):
    stages = {"stage1": False, "stage2": True, "stage3": True}
    config_manager = setup
    config_manager.update_config(pipeline=stages)

    assert config_manager.pipeline == stages


@mock.patch("ska_sdp_pipelines.framework.model.config_manager.yaml")
@mock.patch("builtins.open")
def test_should_update_the_stage_with_cli_over_yaml(
    open_mock, yaml_mock, setup
):

    pipeline = {"stage1": False, "stage2": True, "stage3": True}
    parameters = {
        "stage1": {"stage1_parameter_1": 0},
        "stage2": {"stage2_parameter_1": 0},
        "stage3": {"stage3_parameter_1": 10},
    }
    yaml_config = {"parameters": parameters, "pipeline": pipeline}
    yaml_data = yaml_mock.dump(yaml_config)
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = yaml_data
    yaml_mock.safe_load.return_value = yaml_config
    open_mock.return_value = enter_mock
    config_manager = setup
    cli_stages = {"stage1": True, "stage2": False, "stage3": True}
    config_manager = setup

    config_manager.update_config(
        config_path="/path/to/yaml", pipeline=cli_stages
    )

    assert config_manager.pipeline == cli_stages


def test_should_return_stages_to_run(setup):
    config_manager = setup
    expected_stages_to_run = ["stage1", "stage3"]

    assert expected_stages_to_run == config_manager.stages_to_run


def test_should_return_stages_config(setup):
    config_manager = setup
    expected_stage_config = {"stage2_parameter_1": 0}

    assert expected_stage_config == config_manager.stage_config("stage2")


@mock.patch("ska_sdp_pipelines.framework.model.config_manager.write_yml")
def test_should_write_config_to_path(write_yml_mock, setup):
    config_manager = setup

    path = "/path/to/write"
    config = {
        "pipeline": {"stage1": True, "stage2": False, "stage3": True},
        "parameters": {
            "stage1": {"stage1_parameter_1": 0},
            "stage2": {"stage2_parameter_1": 0},
            "stage3": {"stage3_parameter_1": 0},
        },
        "global_parameters": {
            "global_param_1": 1,
            "global_param_2": 1,
        },
    }

    config_manager.write_yml(path)

    write_yml_mock.assert_called_once_with(path, config)
