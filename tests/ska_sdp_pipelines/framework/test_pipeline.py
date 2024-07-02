from mock import Mock, mock

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

    pipeline("infile_path")
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
