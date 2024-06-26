from mock import Mock, mock

from ska_sdp_pipelines.framework.pipeline import Pipeline


@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_name",
    return_value="output_name",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
def test_should_run_the_pipeline(write_mock, create_output_mock, read_mock):
    stage = Mock("mock_stage", return_value="Stage output")
    pipeline = Pipeline("test_pipeline", stage)

    pipeline("infile_path")
    read_mock.assert_called_once_with("infile_path")
    stage.assert_called_once_with("dataset")
    create_output_mock.assert_called_once_with("infile_path", "test_pipeline")
    write_mock.assert_called_once_with("Stage output", "output_name")


def test_should_get_instance_of_pipeline():
    pipeline = Pipeline("test_pipeline", "stage")
    assert pipeline == pipeline.get_instance()
