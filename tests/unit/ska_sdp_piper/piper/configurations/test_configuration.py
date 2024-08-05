import pytest
from mock import Mock

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.exceptions import (
    ArgumentMismatchException,
    PipelineMetadataMissingException,
)


def test_should_return_config_dictionary():
    configurations = Configuration(
        a=ConfigParam("number", 10), b=ConfigParam("number", 20)
    )

    default_config = configurations.items
    assert default_config == {"a": 10, "b": 20}


def test_should_extend_configurable_arguments_with_actual_values():
    configurations = Configuration(
        a=ConfigParam("number", 10), b=ConfigParam("number", 20)
    )

    extended_args = configurations.extend(a=20, c=30)
    assert extended_args == {"a": 20, "b": 20, "c": 30}


def test_should_raise_exception_if_mandatory_first_argument_is_missing():
    configuration = Configuration()
    configuration_with_args = Configuration(
        stage_arguments=ConfigParam("number", 0)
    )

    temp_stage = Mock(name="temp_stage")
    temp_stage.params = []

    temp_stage1 = Mock(name="temp_stage1")
    temp_stage1.params = ["stage_arguments"]

    with pytest.raises(PipelineMetadataMissingException):
        configuration.valididate_arguments_for(temp_stage)

    with pytest.raises(PipelineMetadataMissingException):
        configuration_with_args.valididate_arguments_for(temp_stage1)


def test_should_raise_exception_if_function_arguments_are_invalide():
    config = Configuration(stage_arguments=ConfigParam("number", 0))

    temp_stage = Mock(name="temp_stage")
    temp_stage.params = ["vis"]

    with pytest.raises(ArgumentMismatchException):
        config.valididate_arguments_for(temp_stage)
