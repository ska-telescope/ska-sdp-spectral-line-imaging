import pytest
from mock import Mock, mock

from ska_sdp_pipelines.framework.configuration import Configuration
from ska_sdp_pipelines.framework.exceptions import (
    NoStageToExecuteException,
    StageNotFoundException,
)
from ska_sdp_pipelines.framework.pipeline import Pipeline


@pytest.fixture(scope="function", autouse=True)
def log_util():
    with mock.patch(
        "ska_sdp_pipelines.framework.pipeline.LogUtil"
    ) as log_util_mock:
        yield log_util_mock


@pytest.fixture(scope="function")
def default_scheduler():
    with mock.patch(
        "ska_sdp_pipelines.framework.scheduler.DefaultScheduler"
    ) as default_scheduler_mock:

        default_scheduler_mock.execute.return_value = "output"

        yield default_scheduler_mock


@pytest.fixture(scope="function", autouse=True)
def scheduler_factory(default_scheduler):
    with mock.patch(
        "ska_sdp_pipelines.framework.pipeline.SchedulerFactory"
    ) as scheduler_factory_mock:
        scheduler_factory_mock.get_scheduler.return_value = default_scheduler
        yield scheduler_factory_mock


@pytest.fixture(scope="function", autouse=True)
def read_mock():
    with mock.patch(
        "ska_sdp_pipelines.framework.pipeline.read_dataset",
        return_value="dataset",
    ) as read:
        yield read


@pytest.fixture(scope="function", autouse=True)
def write_mock():
    with mock.patch(
        "ska_sdp_pipelines.framework.pipeline.write_dataset"
    ) as write:
        yield write


@pytest.fixture(scope="function", autouse=True)
def create_output_mock():
    with mock.patch(
        "ska_sdp_pipelines.framework.pipeline.create_output_name",
        return_value="output_name",
    ) as create_output:
        yield create_output


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_run_the_pipeline(
    config_manager_mock,
    read_mock,
    write_mock,
    create_output_mock,
    scheduler_factory,
    default_scheduler,
):
    config_manager_mock.return_value = config_manager_mock

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()

    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2])

    pipeline("infile_path", [])

    read_mock.assert_called_once_with("infile_path")
    scheduler_factory.get_scheduler.assert_called_once_with(None)
    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage2], "dataset", config_manager_mock, False
    )

    create_output_mock.assert_called_once_with("infile_path", "test_pipeline")
    write_mock.assert_called_once_with("output", "output_name")


def test_should_run_the_pipeline_with_verbose(log_util):
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()

    pipeline = Pipeline("test_pipeline", stages=[stage1])
    pipeline("infile_path", [], verbose=True)

    log_util.configure.assert_has_calls(
        [mock.call("test_pipeline"), mock.call("test_pipeline", True)]
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_run_the_pipeline_with_selected_stages(
    config_manager_mock, default_scheduler, read_mock
):
    config_manager_mock.return_value = config_manager_mock
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.stage_config = Configuration()
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline("infile_path", ["stage1", "stage3"])
    read_mock.assert_called_once_with("infile_path")

    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage3], "dataset", config_manager_mock, False
    )


def test_should_instantiate_dask_client(scheduler_factory):
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    pipeline = Pipeline("test_pipeline", stages=[stage1])
    dask_scheduler_address = "some_ip"
    pipeline("infile_path", dask_scheduler=dask_scheduler_address)
    scheduler_factory.get_scheduler.assert_called_once_with(
        dask_scheduler_address
    )


def test_should_not_run_if_no_stages_are_provided():
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    pipeline = Pipeline("test_pipeline")
    dask_scheduler_address = "some_ip"
    with pytest.raises(NoStageToExecuteException):
        pipeline("infile_path", dask_scheduler=dask_scheduler_address)


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_run_the_pipeline_with_selected_stages_from_config(
    config_manager_mock, default_scheduler
):
    config_manager_mock.get_config.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage3"]

    stage1 = Mock("mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage2 = Mock("mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage3 = Mock("mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.stage_config = Configuration()
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline("infile_path", config_path="/path/to/config")

    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage3], "dataset", config_manager_mock, False
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_run_the_pipeline_with_stages_from_cli_over_config(
    config_manager_mock, default_scheduler
):
    config_manager_mock.get_config.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage2"]

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.stage_config = Configuration()
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline(
        "infile_path", ["stage1", "stage3"], config_path="/path/to/config"
    )

    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage3], "dataset", config_manager_mock, False
    )


def test_should_raise_exception_if_wrong_stage_is_provided():
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.stage_config = Configuration()
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
    pipeline = Pipeline("test_pipeline", ["stage"])
    assert pipeline == pipeline.get_instance()
