import pytest
from mock import MagicMock, Mock, mock

from ska_sdp_pipelines.framework.configuration import Configuration
from ska_sdp_pipelines.framework.exceptions import (
    NoStageToExecuteException,
    StageNotFoundException,
)
from ska_sdp_pipelines.framework.pipeline import Pipeline


@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.dask.compute", return_value="output"
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.dask.delayed")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_dir",
    return_value="./output/timestamp",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_yml")
def test_should_run_the_pipeline(
    write_yml_mock,
    write_mock,
    create_output_mock,
    read_mock,
    delayed_mock,
    compute_mock,
):
    delayed_mock_output = Mock(name="delayed")

    delayed_mock_call_1 = Mock(name="delay1", return_value="DELAYED_1")
    delayed_mock_call_2 = Mock(name="delay2", return_value=delayed_mock_output)
    delayed_mock.side_effect = [delayed_mock_call_1, delayed_mock_call_2]
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage1.config = {"stage1": "stage1_config"}
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage2.config = {"stage2": "stage2_config"}
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2])

    pipeline("infile_path", [])
    read_mock.assert_called_once_with("infile_path")
    delayed_mock.assert_has_calls([mock.call(stage1), mock.call(stage2)])

    expected_config = {
        "pipeline": {"stage1": True, "stage2": True},
        "parameters": {
            "stage1": "stage1_config",
            "stage2": "stage2_config",
        },
    }

    delayed_mock_call_1.assert_called_once_with(
        {
            "input_data": "dataset",
            "output": None,
            "output_dir": "./output/timestamp",
        }
    )
    delayed_mock_call_2.assert_called_once_with(
        {
            "input_data": "dataset",
            "output": "DELAYED_1",
            "output_dir": "./output/timestamp",
        }
    )

    compute_mock.assert_called_once_with("DELAYED_1", delayed_mock_output)

    create_output_mock.assert_called_once_with("./output", "test_pipeline")
    write_mock.assert_called_once_with("output", "./output/timestamp")
    write_yml_mock.assert_called_once_with(
        "./output/timestamp/config.yml", expected_config
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.dask.delayed")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_dir",
    return_value="./output/timestamp",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_yml")
def test_should_run_the_pipeline_with_selected_stages(
    write_yml_mock, write_mock, create_output_mock, read_mock, delayed_mock
):
    delayed_mock_output = Mock(name="delayed")
    delayed_mock_output.compute = Mock(name="compute", return_value="output")

    delayed_mock_call_1 = Mock(name="delay1", return_value="DELAYED_1")
    delayed_mock_call_2 = Mock(name="delay3", return_value=delayed_mock_output)
    delayed_mock.side_effect = [delayed_mock_call_1, delayed_mock_call_2]
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage1.config = {"stage1": "stage1_config"}
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage2.config = {"stage2": "stage2_config"}
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.config = {"stage3": "stage3_config"}
    stage3.stage_config = Configuration()
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline("infile_path", ["stage1", "stage3"])
    read_mock.assert_called_once_with("infile_path")
    delayed_mock.assert_has_calls([mock.call(stage1), mock.call(stage3)])


@mock.patch("ska_sdp_pipelines.framework.pipeline.Client")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_dir",
    return_value="./output/timestamp",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_yml")
def test_should_instantiate_dask_client(
    write_yml_mock, write_mock, create_output_mock, read_mock, client_mock
):
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage1.config = {"stage1": "stage1_config"}
    pipeline = Pipeline("test_pipeline", stages=[stage1])
    dask_scheduler_address = "some_ip"
    pipeline("infile_path", dask_scheduler=dask_scheduler_address)
    client_mock.assert_called_once_with(dask_scheduler_address)


@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_dir",
    return_value="./output/timestamp",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_yml")
def test_should_not_run_if_no_stages_are_provided(
    write_yml_mock, create_output_mock, read_mock
):
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()

    pipeline = Pipeline("test_pipeline")
    dask_scheduler_address = "some_ip"
    with pytest.raises(NoStageToExecuteException):
        pipeline("infile_path", dask_scheduler=dask_scheduler_address)


@mock.patch("ska_sdp_pipelines.framework.model.config_manager.yaml")
@mock.patch("ska_sdp_pipelines.framework.pipeline.dask.delayed")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_dir",
    return_value="./output/timestamp",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_yml")
@mock.patch("ska_sdp_pipelines.framework.pipeline.shutil")
@mock.patch("builtins.open")
def test_should_run_the_pipeline_with_selected_stages_from_config(
    open_mock,
    shutil_mock,
    write_yml_mock,
    write_mock,
    create_output_mock,
    read_mock,
    delayed_mock,
    yaml_mock,
):
    yaml_mock.safe_load.return_value = {
        "pipeline": {"stage1": True, "stage2": False, "stage3": True}
    }

    delayed_mock_output = Mock(name="delayed")
    delayed_mock_output.compute = Mock(name="compute", return_value="output")

    delayed_mock_call_1 = Mock(name="delay1", return_value="DELAYED_1")
    delayed_mock_call_2 = Mock(name="delay3", return_value=delayed_mock_output)
    delayed_mock.side_effect = [delayed_mock_call_1, delayed_mock_call_2]
    stage1 = Mock("mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage1.config = {"stage1": "stage1_config"}
    stage2 = Mock("mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage2.config = {"stage2": "stage2_config"}
    stage3 = Mock("mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.stage_config = Configuration()
    stage3.config = {"stage3": "stage3_config"}
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline("infile_path", config_path="/path/to/config")
    shutil_mock.copy.assert_called_once_with(
        "/path/to/config", "./output/timestamp/config.yml"
    )

    delayed_mock.assert_has_calls([mock.call(stage1), mock.call(stage3)])


@mock.patch("ska_sdp_pipelines.framework.model.config_manager.yaml")
@mock.patch("ska_sdp_pipelines.framework.pipeline.dask.delayed")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_dir",
    return_value="./output/timestamp",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
@mock.patch("builtins.open")
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_yml")
@mock.patch("ska_sdp_pipelines.framework.pipeline.shutil")
def test_should_run_the_pipeline_with_stages_from_cli_over_config(
    shutil_mock,
    write_yml_mock,
    open_mock,
    write_mock,
    create_output_mock,
    read_mock,
    delayed_mock,
    yaml_mock,
):
    file_obj = Mock(name="file_obj")
    file_obj.write = Mock(name="readlines")
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = file_obj
    open_mock.return_value = enter_mock

    yaml_mock.safe_load.return_value = {
        "pipeline": {"stage1": True, "stage2": True, "stage3": False}
    }

    delayed_mock_output = Mock(name="delayed")
    delayed_mock_output.compute = Mock(name="compute", return_value="output")

    delayed_mock_call_1 = Mock(name="delay1", return_value="DELAYED_1")
    delayed_mock_call_2 = Mock(name="delay3", return_value=delayed_mock_output)
    delayed_mock.side_effect = [delayed_mock_call_1, delayed_mock_call_2]
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage1.config = {"stage1": "stage1_config"}
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.config = {"stage2": "stage2_config"}
    stage2.stage_config = Configuration()
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.stage_config = Configuration()
    stage3.config = {"stage3": "stage3_config"}
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline(
        "infile_path", ["stage1", "stage3"], config_path="/path/to/config"
    )
    open_mock.assert_called_once_with("/path/to/config", "r")

    delayed_mock.assert_has_calls([mock.call(stage1), mock.call(stage3)])


@mock.patch("ska_sdp_pipelines.framework.model.config_manager.yaml")
@mock.patch("ska_sdp_pipelines.framework.pipeline.dask.delayed")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_dir",
    return_value="./output/timestamp",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_yml")
@mock.patch("ska_sdp_pipelines.framework.pipeline.shutil")
@mock.patch("builtins.open")
def test_should_run_pass_configuration_params_for_stages(
    open_mock,
    shutil_mock,
    write_yml_mock,
    write_mock,
    create_output_mock,
    read_mock,
    delayed_mock,
    yaml_mock,
):
    file_obj = Mock(name="file_obj")
    file_obj.write = Mock(name="readlines")
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = file_obj
    open_mock.return_value = enter_mock

    yaml_mock.safe_load.return_value = {
        "pipeline": {"stage1": True, "stage2": True, "stage3": True},
        "parameters": {
            "stage1": {"stage1_parameter_1": 0},
            "stage2": {"stage2_parameter_1": 0},
            "stage3": {"stage3_parameter_1": 0},
        },
    }

    delayed_mock_call_1 = Mock(name="delay1", return_value="DELAYED_1")
    delayed_mock_call_2 = Mock(name="delay2", return_value="DELAYED_2")
    delayed_mock_call_3 = Mock(name="delay3", return_value="delayed_call")

    delayed_mock.side_effect = [
        delayed_mock_call_1,
        delayed_mock_call_2,
        delayed_mock_call_3,
    ]
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage1.config = {"stage1": "stage1_config"}
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage2.config = {"stage2": "stage2_config"}
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.stage_config = Configuration()
    stage3.config = {"stage3": "stage3_config"}
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline("infile_path", config_path="/path/to/config")
    open_mock.assert_called_once_with("/path/to/config", "r")

    delayed_mock_call_1.assert_called_once_with(
        {
            "input_data": "dataset",
            "output": None,
            "output_dir": "./output/timestamp",
        },
        stage1_parameter_1=0,
    )
    delayed_mock_call_2.assert_called_once_with(
        {
            "input_data": "dataset",
            "output": "DELAYED_1",
            "output_dir": "./output/timestamp",
        },
        stage2_parameter_1=0,
    )
    delayed_mock_call_3.assert_called_once_with(
        {
            "input_data": "dataset",
            "output": "DELAYED_2",
            "output_dir": "./output/timestamp",
        },
        stage3_parameter_1=0,
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.dask.delayed")
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.read_dataset", return_value="dataset"
)
@mock.patch(
    "ska_sdp_pipelines.framework.pipeline.create_output_dir",
    return_value="./output/timestamp",
)
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_dataset")
@mock.patch("ska_sdp_pipelines.framework.pipeline.write_yml")
def test_should_raise_exception_if_wrong_stage_is_provided(
    write_yml_mock, write_mock, create_output_mock, read_mock, delayed_mock
):
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage1.config = {"stage1": "stage1_config"}
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage2.config = {"stage2": "stage2_config"}
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.stage_config = Configuration()
    stage3.config = {"stage3": "stage3_config"}
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    with pytest.raises(StageNotFoundException):
        pipeline("infile_path", ["stage1", "stage5"])


def test_should_return_pipeline_defualt_configuration():
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {"stage1": "stage1_config"}

    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.config = {"stage2": "stage2_config"}

    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
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


def test_should_get_instance_of_pipeline():
    pipeline = Pipeline("test_pipeline", "stage")
    assert pipeline == pipeline.get_instance()
