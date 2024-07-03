import pytest
from mock import Mock, mock

from ska_sdp_pipelines.framework.exceptions import StageNotFoundException
from ska_sdp_pipelines.framework.pipeline import Pipeline


@mock.patch("ska_sdp_pipelines.framework.pipeline.dask.delayed")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_name",
    return_value="output_name",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
def test_should_run_the_pipeline(
    write_mock, create_output_mock, read_mock, delayed_mock
):
    delayed_mock_output = Mock("delayed")
    delayed_mock_output.compute = Mock("compute", return_value="output")

    delayed_mock_call_1 = Mock("delay1", return_value="DELAYED_1")
    delayed_mock_call_2 = Mock("delay2", return_value=delayed_mock_output)
    delayed_mock.side_effect = [delayed_mock_call_1, delayed_mock_call_2]
    stage1 = Mock("mock_stage_1", return_value="Stage_1 output")
    stage2 = Mock("mock_stage_2", return_value="Stage_2 output")
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2])

    pipeline("infile_path", [])
    read_mock.assert_called_once_with("infile_path")
    delayed_mock.assert_has_calls(
        [mock.call(pipeline.execute_stage), mock.call(pipeline.execute_stage)]
    )

    delayed_mock_call_1.assert_called_once_with(
        stage1, {"input_data": "dataset"}
    )
    delayed_mock_call_2.assert_called_once_with(stage2, "DELAYED_1")

    delayed_mock_output.compute.assert_called_once()

    create_output_mock.assert_called_once_with("infile_path", "test_pipeline")
    write_mock.assert_called_once_with("output", "output_name")


@mock.patch("ska_sdp_pipelines.framework.pipeline.dask.delayed")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_name",
    return_value="output_name",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
def test_should_run_the_pipeline_with_selected_stages(
    write_mock, create_output_mock, read_mock, delayed_mock
):
    delayed_mock_output = Mock("delayed")
    delayed_mock_output.compute = Mock("compute", return_value="output")

    delayed_mock_call_1 = Mock("delay1", return_value="DELAYED_1")
    delayed_mock_call_2 = Mock("delay3", return_value=delayed_mock_output)
    delayed_mock.side_effect = [delayed_mock_call_1, delayed_mock_call_2]
    stage1 = Mock("mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage2 = Mock("mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage3 = Mock("mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline("infile_path", ["stage1", "stage3"])
    read_mock.assert_called_once_with("infile_path")
    delayed_mock.assert_has_calls(
        [mock.call(pipeline.execute_stage), mock.call(pipeline.execute_stage)]
    )

    delayed_mock_call_1.assert_called_once_with(
        stage1, {"input_data": "dataset"}
    )
    delayed_mock_call_2.assert_called_once_with(stage3, "DELAYED_1")

    delayed_mock_output.compute.assert_called_once()

    create_output_mock.assert_called_once_with("infile_path", "test_pipeline")
    write_mock.assert_called_once_with("output", "output_name")


@mock.patch("ska_sdp_pipelines.framework.pipeline.dask.delayed")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_name",
    return_value="output_name",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
def test_should_raise_exception_if_wrong_stage_is_provided(
    write_mock, create_output_mock, read_mock, delayed_mock
):
    stage1 = Mock("mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage2 = Mock("mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage3 = Mock("mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    with pytest.raises(StageNotFoundException):
        pipeline("infile_path", ["stage1", "stage5"])


def test_should_return_pipeline_defualt_configuration():
    stage1 = Mock("mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {"stage1": "stage1_config"}

    stage2 = Mock("mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.config = {"stage2": "stage2_config"}

    stage3 = Mock("mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.config = {"stage3": "stage3_config"}

    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    expected_config = {
        "pipeline": {"stage1": True, "stage2": True, "stage3": True},
        "parameters": {
            "stage1": "stage1_config",
            "stage2": "stage2_config",
            "stage3": "stage3_config",
        },
    }

    assert pipeline.config == expected_config


def test_should_execute_stage():
    stage1 = Mock("mock_stage_1", return_value="Stage_1 output")

    pipeline = Pipeline("test_pipeline", stages=[stage1])
    pipeline_data = {"input": "input"}
    pipeline.execute_stage(stage1, pipeline_data, "OTHER_ARGS", k="KEY_ARGS")
    stage1.assert_called_once_with(pipeline_data, "OTHER_ARGS", k="KEY_ARGS")

    assert pipeline_data["output"] == "Stage_1 output"


def test_should_get_instance_of_pipeline():
    pipeline = Pipeline("test_pipeline", "stage")
    assert pipeline == pipeline.get_instance()
