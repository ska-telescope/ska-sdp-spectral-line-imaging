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


def test_should_update_additional_parameters(mock_stages):
    stage1_mock, stage2_mock, stage3_mock = mock_stages

    stages = Stages(mock_stages)

    stages.add_additional_parameters(arg1="arg1", arg2="arg2")

    stage1_mock.add_additional_parameters.assert_called_once_with(
        arg1="arg1", arg2="arg2"
    )

    stage3_mock.add_additional_parameters.assert_called_once_with(
        arg1="arg1", arg2="arg2"
    )

    assert stage2_mock.call_count == 0


def test_should_return_stages_with_name(mock_stages):

    stages = Stages(mock_stages)

    assert stages.get_stages(["stage1", "stage2"]) == mock_stages[:2]


def test_should_return_stages_with_name_iterator(mock_stages):

    stages = Stages(mock_stages)

    expected_stages = [stage for stage in stages]

    assert mock_stages == expected_stages


@mock.patch("ska_sdp_piper.piper.stage.stages.inspect.getfullargspec")
def test_should_add_additional_pipeline_params_for_stage(
    argspec_mock,
):
    test = mock.Mock(name="test")

    args_mock = mock.Mock(name="args_mock")
    args_mock.args = ["vis", "config_param", "additional_param_1"]
    argspec_mock.return_value = args_mock

    stage = Stage(
        "test", test, Configuration(config_param=ConfigParam("number", 20))
    )

    stage.add_additional_parameters(
        additional_param_1=40,
        additional_param_2=30,
    )

    stage("UPSTREAM_OUTPUT")

    test.assert_called_once_with(
        "UPSTREAM_OUTPUT", config_param=20, additional_param_1=40
    )


def test_should_raise_exception_if_pipeline_parameters_is_not_initialised():
    def test(vis, config_param, additional_param_1):
        pass

    stage = Stage(
        "test", test, Configuration(config_param=ConfigParam("number", 20))
    )

    with pytest.raises(PipelineMetadataMissingException):
        stage("UPSTREAM_OUTPUT")


def test_should_update_stage_parameters():
    stage1 = Mock(name="stage1")
    stage1.name = "stage1"
    stages = Stages([stage1])

    stages.update_stage_parameters(
        {"stage1": {"a": 10, "b": 20}, "stage2": {"c": 30, "d": 40}}
    )

    stage1.update_parameters.assert_called_once_with(a=10, b=20)


@mock.patch("ska_sdp_piper.piper.stage.stages.inspect.getfullargspec")
def test_should_update_default_stage_config(argspec_mock):
    test = mock.Mock(name="test")

    args_mock = mock.Mock(name="args_mock")
    args_mock.args = ["vis", "param1", "param2", "additional_param_1"]
    argspec_mock.return_value = args_mock

    stage = Stage(
        "test",
        test,
        Configuration(
            param1=ConfigParam(int, 20),
            param2=ConfigParam(str, "Name"),
        ),
    )

    stage.update_parameters(param1=10, param2="New Name")

    assert stage.config == {"test": {"param1": 10, "param2": "New Name"}}
