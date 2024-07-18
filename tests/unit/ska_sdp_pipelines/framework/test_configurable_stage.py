import pytest

from ska_sdp_pipelines.framework.configurable_stage import ConfigurableStage
from ska_sdp_pipelines.framework.configuration import (
    ConfigParam,
    Configuration,
)
from ska_sdp_pipelines.framework.exceptions import (
    ArgumentMismatchException,
    PipelineMetadataMissingException,
)


def test_should_create_a_configurable_stage():
    @ConfigurableStage("temp_stage")
    def temp_stage(vis):
        return vis

    assert temp_stage("vis") == "vis"


def test_should_return_default_config_dictionary():
    @ConfigurableStage(
        "temp_stage",
        Configuration(
            a=ConfigParam("number", 10), b=ConfigParam("number", 20)
        ),
    )
    def temp_stage(input_data, a=None, b=None):
        return (input_data, a, b)

    assert temp_stage.config == {"temp_stage": {"a": 10, "b": 20}}


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
