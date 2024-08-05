import pytest

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.exceptions import (
    ArgumentMismatchException,
    PipelineMetadataMissingException,
)
from ska_sdp_piper.piper.stage import ConfigurableStage


def test_should_create_a_configurable_stage():
    @ConfigurableStage(
        "temp_stage",
        Configuration(
            a=ConfigParam("number", 10), b=ConfigParam("number", 20)
        ),
    )
    def temp_stage(upstream_output, a, b):
        return (upstream_output, a, b)

    assert temp_stage.stage_definition("vis", 30, 40) == ("vis", 30, 40)


def test_should_update_stage_properties_with_the_provided_values_from_config():
    @ConfigurableStage(
        "temp_stage",
        Configuration(
            a=ConfigParam("number", 10), b=ConfigParam("number", 20)
        ),
    )
    def temp_stage(upstream_output, a=None, b=None, input_data=None):
        return (input_data, a, b)

    assert temp_stage.config == {"temp_stage": {"a": 10, "b": 20}}
    assert temp_stage.name == "temp_stage"
    assert temp_stage.params == [
        "upstream_output",
        "a",
        "b",
        "input_data",
    ]


def test_should_raise_exception_if_vis_is_missing_in_args():
    with pytest.raises(PipelineMetadataMissingException):

        @ConfigurableStage("temp_stage")
        def temp_stage():
            pass

    with pytest.raises(PipelineMetadataMissingException):

        @ConfigurableStage(
            "temp_stage",
            Configuration(stage_arguments=ConfigParam("number", 0)),
        )
        def temp_stage1(stage_arguments):
            pass


def test_should_raise_exception_if_function_arguments_are_invalide():
    with pytest.raises(ArgumentMismatchException):

        @ConfigurableStage(
            "temp_stage",
            Configuration(stage_arguments=ConfigParam("number", 0)),
        )
        def temp_stage(vis):
            pass
