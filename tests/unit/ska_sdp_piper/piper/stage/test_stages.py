import logging

import pytest
from mock import Mock, mock

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.exceptions import (
    NoStageToExecuteException,
    PipelineMetadataMissingException,
    StageNotFoundException,
)
from ska_sdp_piper.piper.stage.stages import Stage, Stages


@pytest.fixture(scope="function")
def mock_stages():
    stage1 = Mock(name="stage1")
    stage1.name = "stage1"

    stage2 = Mock(name="stage2")
    stage2.name = "stage2"

    stage3 = Mock(name="stage3")
    stage3.name = "stage3"

    yield [stage1, stage2, stage3]


def test_should_raise_exception_if_no_stages_are_provided(mock_stages):
    stages = Stages(mock_stages)
    with pytest.raises(NoStageToExecuteException):
        stages.validate([])


def test_should_raise_exception_if_invalid_stages_are_provided(mock_stages):
    stages = Stages(mock_stages)
    with pytest.raises(StageNotFoundException):
        stages.validate(["stage4"])


def test_should_validate_stages(mock_stages):
    stages = Stages(mock_stages)
    stages.validate(["stage3", "stage2"])


def test_should_update_stage_pipeline_parameters(mock_stages):
    stage1_mock, stage2_mock, stage3_mock = mock_stages

    stages = Stages(mock_stages)

    stage_configs = {"stage1": "STAGE_CONFIG_1", "stage3": "STAGE_CONFIG_3"}

    stages.update_pipeline_parameters(
        ["stage1", "stage3"], stage_configs, arg1="arg1", arg2="arg2"
    )

    stage1_mock.update_pipeline_parameters.assert_called_once_with(
        "STAGE_CONFIG_1", arg1="arg1", arg2="arg2"
    )

    stage3_mock.update_pipeline_parameters.assert_called_once_with(
        "STAGE_CONFIG_3", arg1="arg1", arg2="arg2"
    )

    assert stage2_mock.call_count == 0


def test_should_return_stages_with_name(mock_stages):

    stages = Stages(mock_stages)

    assert stages.get_stages(["stage1", "stage2"]) == mock_stages[:2]


def test_should_return_stages_with_name_iterator(mock_stages):

    stages = Stages(mock_stages)

    expected_stages = [stage for stage in stages]

    assert mock_stages == expected_stages


@mock.patch("ska_sdp_piper.piper.stage.stages.logging.getLogger")
@mock.patch("ska_sdp_piper.piper.stage.stages.inspect.getfullargspec")
def test_should_update_pipeline_params_for_stage_and_return_the_stage_args(
    argspec_mock, get_logger_mock
):
    logger_mock = Mock(name="logger")
    get_logger_mock.return_value = logger_mock
    test = mock.Mock(name="test")

    args_mock = mock.Mock(name="args_mock")
    args_mock.args = ["vis", "config_param", "additional_param_1"]
    argspec_mock.return_value = args_mock

    stage = Stage(
        "test", test, Configuration(config_param=ConfigParam("number", 20))
    )

    stage.update_pipeline_parameters(
        config={"config_param": 30},
        additional_param_1=40,
        additional_param_2=30,
    )

    stage("UPSTREAM_OUTPUT")

    test.assert_called_once_with(
        "UPSTREAM_OUTPUT", config_param=30, additional_param_1=40
    )
    logger_mock.setLevel.assert_has_calls([mock.call(logging.INFO)])


@mock.patch("ska_sdp_piper.piper.stage.stages.logging.getLogger")
@mock.patch("ska_sdp_piper.piper.stage.stages.inspect.getfullargspec")
def test_should_execute_stage_with_verbosity(argspec_mock, get_logger_mock):
    logger_mock = Mock(name="logger")
    get_logger_mock.return_value = logger_mock

    test = mock.Mock(name="test")

    args_mock = mock.Mock(name="args_mock")
    args_mock.args = ["vis", "config_param", "additional_param_1"]
    argspec_mock.return_value = args_mock

    stage = Stage(
        "test", test, Configuration(config_param=ConfigParam("number", 20))
    )

    stage.update_pipeline_parameters(
        config={"config_param": 30},
        additional_param_1=40,
        additional_param_2=30,
    )

    stage("UPSTREAM_OUTPUT", verbose=True)

    logger_mock.setLevel.assert_has_calls(
        [mock.call(logging.INFO), mock.call(logging.DEBUG)]
    )


def test_should_raise_exception_if_pipeline_parameters_is_not_initialised():
    def test(vis, config_param, additional_param_1):
        pass

    stage = Stage(
        "test", test, Configuration(config_param=ConfigParam("number", 20))
    )

    with pytest.raises(PipelineMetadataMissingException):
        stage("UPSTREAM_OUTPUT")
