import pytest
from mock import Mock

from ska_sdp_piper.piper.exceptions import (
    NoStageToExecuteException,
    StageNotFoundException,
)
from ska_sdp_piper.piper.model.stages import Stages


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
