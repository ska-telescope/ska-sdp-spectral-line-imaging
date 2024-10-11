import pytest
from mock import mock

from ska_sdp_piper.piper.configurations.config_manager import ConfigManager
from ska_sdp_piper.piper.exceptions import StageNotFoundException


@pytest.fixture(autouse=True)
def setup():
    pipeline = {"stage1": True, "stage2": False, "stage3": True}
    parameters = {
        "stage1": {"stage1_parameter_1": 0},
        "stage2": {"stage2_parameter_1": [0, 1, 2]},
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
        "stage2": {"stage2_parameter_1": [0, 1, 2]},
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
        "stage2": {"stage2_parameter_1": [0, 1, 2]},
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


@mock.patch("ska_sdp_piper.piper.configurations.config_manager.read_yml")
def test_should_update_the_default_config_with_yaml(read_yml, setup):
    pipeline = {"stage1": False, "stage2": True, "stage3": True}
    parameters = {
        "stage1": {"stage1_parameter_1": 10},
        "stage2": {"stage2_parameter_1": 20},
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

    read_yml.return_value = yaml_config

    config_manager = setup

    config_manager.update_config("/path/to/yaml")

    expected_parameters = {
        "stage1": {"stage1_parameter_1": 10},
        "stage2": {"stage2_parameter_1": 20},
        "stage3": {"stage3_parameter_1": 0},
    }

    read_yml.assert_called_once_with("/path/to/yaml")

    assert config_manager.pipeline == pipeline
    assert config_manager.parameters == expected_parameters
    assert config_manager.global_parameters == global_parameters


@mock.patch("ska_sdp_piper.piper.configurations.config_manager.read_yml")
def test_should_update_the_default_config_with_yaml_without_pipeline_section(
    read_yml_mock, setup
):
    parameters = {
        "stage1": {"stage1_parameter_1": 0},
        "stage2": {"stage2_parameter_1": [0, 1, 2]},
        "stage3": {"stage3_parameter_1": 10},
    }
    global_parameters = {
        "global_param_1": 20,
        "global_param_2": 25,
    }

    yaml_config = {
        "parameters": parameters,
        "global_parameters": global_parameters,
    }

    read_yml_mock.return_value = yaml_config
    config_manager = setup

    config_manager.update_config("/path/to/yaml")

    read_yml_mock.assert_called_once_with("/path/to/yaml")

    assert config_manager.pipeline == {
        "stage1": True,
        "stage2": False,
        "stage3": True,
    }
    assert config_manager.parameters == parameters
    assert config_manager.global_parameters == global_parameters


def test_should_update_pipeline_states(setup):

    pipeline_states = {"stage1": False, "stage2": False, "stage3": True}
    config_manager = setup

    config_manager.update_pipeline(pipeline_states)

    assert config_manager.pipeline == pipeline_states


def test_should_return_stages_to_run(setup):
    config_manager = setup
    expected_stages_to_run = ["stage1", "stage3"]

    assert expected_stages_to_run == config_manager.stages_to_run


def test_should_return_stages_config(setup):
    config_manager = setup
    expected_stage_config = {"stage2_parameter_1": [0, 1, 2]}

    assert expected_stage_config == config_manager.stage_config("stage2")


@mock.patch("ska_sdp_piper.piper.configurations.config_manager.write_yml")
def test_should_write_config_to_path(write_yml_mock, setup):
    config_manager = setup

    path = "/path/to/write"
    config = {
        "pipeline": {"stage1": True, "stage2": False, "stage3": True},
        "parameters": {
            "stage1": {"stage1_parameter_1": 0},
            "stage2": {"stage2_parameter_1": [0, 1, 2]},
            "stage3": {"stage3_parameter_1": 0},
        },
        "global_parameters": {
            "global_param_1": 1,
            "global_param_2": 1,
        },
    }

    config_manager.write_yml(path)

    write_yml_mock.assert_called_once_with(path, config)


def test_should_update_key_in_config(setup):
    config_manager = setup
    config_manager.set("parameters.stage1.stage1_parameter_1", 10)
    config_manager.set("parameters.stage3.stage3_parameter_1", 20)

    assert config_manager.config == {
        "pipeline": {"stage1": True, "stage2": False, "stage3": True},
        "parameters": {
            "stage1": {"stage1_parameter_1": 10},
            "stage2": {"stage2_parameter_1": [0, 1, 2]},
            "stage3": {"stage3_parameter_1": 20},
        },
        "global_parameters": {
            "global_param_1": 1,
            "global_param_2": 1,
        },
    }


def test_should_update_array_in_config(setup):
    config_manager = setup
    config_manager.set("parameters.stage2.stage2_parameter_1", [10, 20, 30])

    assert config_manager.config == {
        "pipeline": {"stage1": True, "stage2": False, "stage3": True},
        "parameters": {
            "stage1": {"stage1_parameter_1": 0},
            "stage2": {"stage2_parameter_1": [10, 20, 30]},
            "stage3": {"stage3_parameter_1": 0},
        },
        "global_parameters": {
            "global_param_1": 1,
            "global_param_2": 1,
        },
    }


def test_should_throw_error_for_non_existent_path_in_config(setup):
    config_manager = setup
    with pytest.raises(ValueError):
        config_manager.set(
            "parameters.stage40.stage2_parameter_1", [10, 20, 30]
        )


def test_should_throw_error_for_non_existent_toplevel_path_in_config(setup):
    config_manager = setup
    with pytest.raises(KeyError):
        config_manager.set("parameters.stage40", [10, 20, 30])


def test_should_throw_error_when_value_type_is_changed(setup):
    config_manager = setup
    with pytest.raises(ValueError):
        config_manager.set(
            "parameters.stage1.stage1_parameter_1", "wrong-type-value"
        )


def test_should_not_throw_error_when_global_var_is_added(setup):
    config_manager = setup
    config_manager.set("global_parameters", {"env": "some-env-var"})

    assert config_manager.config == {
        "pipeline": {"stage1": True, "stage2": False, "stage3": True},
        "parameters": {
            "stage1": {"stage1_parameter_1": 0},
            "stage2": {"stage2_parameter_1": [0, 1, 2]},
            "stage3": {"stage3_parameter_1": 0},
        },
        "global_parameters": {
            "global_param_1": 1,
            "global_param_2": 1,
            "env": "some-env-var",
        },
    }


def test_should_not_throw_error_when_stage_var_is_added(setup):
    config_manager = setup
    config_manager.set("pipeline.stage1", False)

    assert config_manager.config == {
        "pipeline": {"stage1": False, "stage2": False, "stage3": True},
        "parameters": {
            "stage1": {"stage1_parameter_1": 0},
            "stage2": {"stage2_parameter_1": [0, 1, 2]},
            "stage3": {"stage3_parameter_1": 0},
        },
        "global_parameters": {
            "global_param_1": 1,
            "global_param_2": 1,
        },
    }


def test_should_validate_stage_overrides(setup):
    config_manager = setup
    with pytest.raises(ValueError):
        config_manager.set("pipeline.stage1", 10)

    with pytest.raises(StageNotFoundException):
        config_manager.set("pipeline.stag40", True)

    with pytest.raises(ValueError):
        config_manager.set("pipeline.stage1.param1", True)


def test_should_throw_error_when_unknown_namespace_is_added(setup):
    config_manager = setup
    with pytest.raises(ValueError):
        config_manager.set("x.y,z", False)


def test_should_be_able_to_set_param_with_default_none():
    pipeline = {"stage1": True}
    parameters = {"stage1": {"parameter": None}}
    global_parameters = {}

    config_manager = ConfigManager(pipeline, parameters, global_parameters)
    config_manager.set("parameters.stage1.parameter", 10)

    assert config_manager.config == {
        "parameters": {
            "stage1": {"parameter": 10},
        },
        "pipeline": {"stage1": True},
        "global_parameters": {},
    }
