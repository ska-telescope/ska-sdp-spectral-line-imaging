import pytest
from mock import mock

from ska_sdp_piper.piper.configurations.runtime_config import RuntimeConfig
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
    yield RuntimeConfig(pipeline, parameters, global_parameters)


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
    runtime_config = setup

    assert runtime_config.pipeline == pipeline
    assert runtime_config.parameters == parameters
    assert runtime_config.global_parameters == global_parameters


@mock.patch("ska_sdp_piper.piper.configurations.runtime_config.read_yml")
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

    runtime_config = setup

    runtime_config.update_from_yaml("/path/to/yaml")

    expected_parameters = {
        "stage1": {"stage1_parameter_1": 10},
        "stage2": {"stage2_parameter_1": 20},
        "stage3": {"stage3_parameter_1": 0},
    }

    read_yml.assert_called_once_with("/path/to/yaml")

    assert runtime_config.pipeline == pipeline
    assert runtime_config.parameters == expected_parameters
    assert runtime_config.global_parameters == global_parameters


def test_should_handle_empty_update_values(setup):
    expected_pipeline = {"stage1": True, "stage2": False, "stage3": True}
    expected_parameters = {
        "stage1": {"stage1_parameter_1": 0},
        "stage2": {"stage2_parameter_1": [0, 1, 2]},
        "stage3": {"stage3_parameter_1": 0},
    }
    expected_global_parameters = {
        "global_param_1": 1,
        "global_param_2": 1,
    }

    runtime_config = setup
    runtime_config.update_from_yaml("")

    assert runtime_config.parameters == expected_parameters
    assert runtime_config.pipeline == expected_pipeline
    assert runtime_config.global_parameters == expected_global_parameters

    runtime_config.update_from_cli_overrides(None)

    assert runtime_config.parameters == expected_parameters
    assert runtime_config.pipeline == expected_pipeline
    assert runtime_config.global_parameters == expected_global_parameters

    runtime_config.update_from_cli_stages(None)

    assert runtime_config.parameters == expected_parameters
    assert runtime_config.pipeline == expected_pipeline
    assert runtime_config.global_parameters == expected_global_parameters


def test_should_update_the_default_config_with_cli_overrides(setup):
    runtime_config = setup

    runtime_config.update_from_cli_overrides(
        [["parameters.stage1.stage1_parameter_1", "20"]]
    )

    expected_parameters = {
        "stage1": {"stage1_parameter_1": 20},
        "stage2": {"stage2_parameter_1": [0, 1, 2]},
        "stage3": {"stage3_parameter_1": 0},
    }

    assert runtime_config.parameters == expected_parameters


def test_should_update_pipeline_stages(setup):

    pipeline_stages = ["stage3"]
    runtime_config = setup

    runtime_config.update_from_cli_stages(pipeline_stages)

    assert runtime_config.pipeline == {
        "stage1": False,
        "stage2": False,
        "stage3": True,
    }


def test_should_return_stages_to_run(setup):
    runtime_config = setup
    expected_stages_to_run = ["stage1", "stage3"]

    assert expected_stages_to_run == runtime_config.stages_to_run


@mock.patch("ska_sdp_piper.piper.configurations.runtime_config.write_yml")
def test_should_write_config_to_path(write_yml_mock, setup):
    runtime_config = setup

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

    runtime_config.write_yml(path)

    write_yml_mock.assert_called_once_with(path, config)


def test_should_update_key_in_config(setup):
    runtime_config = setup
    runtime_config.set("parameters.stage1.stage1_parameter_1", 10)
    runtime_config.set("parameters.stage3.stage3_parameter_1", 20)
    runtime_config.set("global_parameters", {"env": "some-env-var"})
    runtime_config.set("pipeline.stage1", False)

    assert runtime_config.pipeline == {
        "stage1": False,
        "stage2": False,
        "stage3": True,
    }
    assert runtime_config.parameters == {
        "stage1": {"stage1_parameter_1": 10},
        "stage2": {"stage2_parameter_1": [0, 1, 2]},
        "stage3": {"stage3_parameter_1": 20},
    }
    assert runtime_config.global_parameters == {"env": "some-env-var"}


def test_should_throw_error_for_non_existent_path_in_config(setup):
    runtime_config = setup
    with pytest.raises(StageNotFoundException):
        runtime_config.set(
            "parameters.stage40.stage2_parameter_1", [10, 20, 30]
        )

    with pytest.raises(ValueError):
        runtime_config.set(
            "parameters.stage2.stage2_parameter_4.underfined_param", 1
        )


def test_should_validate_stage_overrides(setup):
    runtime_config = setup
    with pytest.raises(ValueError):
        runtime_config.set("pipeline.stage1", 10)

    with pytest.raises(StageNotFoundException):
        runtime_config.set("pipeline.stag40", True)

    with pytest.raises(ValueError):
        runtime_config.set("pipeline.stage1.param1", True)


def test_should_throw_error_when_unknown_namespace_is_added(setup):
    runtime_config = setup
    with pytest.raises(ValueError):
        runtime_config.set("x.y,z", False)


# def test_should_be_able_to_set_param_with_default_none():
#     pipeline = {"stage1": True}
#     parameters = {"stage1": {"parameter": None}}
#     global_parameters = {}

#     runtime_config = RuntimeConfig(pipeline, parameters, global_parameters)
#     runtime_config.set("parameters.stage1.parameter", 10)

#     assert runtime_config.config == {
#         "parameters": {
#             "stage1": {"parameter": 10},
#         },
#         "pipeline": {"stage1": True},
#         "global_parameters": {},
#     }
