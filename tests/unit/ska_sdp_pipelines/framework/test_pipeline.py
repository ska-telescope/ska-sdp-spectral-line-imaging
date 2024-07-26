import pytest
from mock import Mock, mock
from mock.mock import call

from ska_sdp_pipelines.framework.configuration import Configuration
from ska_sdp_pipelines.framework.constants import (
    CONFIG_CLI_ARGS,
    MANDATORY_CLI_ARGS,
)
from ska_sdp_pipelines.framework.exceptions import (
    NoStageToExecuteException,
    StageNotFoundException,
)
from ska_sdp_pipelines.framework.model.cli_command import CLIArgument
from ska_sdp_pipelines.framework.pipeline import Pipeline


@pytest.fixture(scope="function", autouse=True)
def log_util():
    with mock.patch(
        "ska_sdp_pipelines.framework.pipeline.LogUtil"
    ) as log_util_mock:
        yield log_util_mock


@pytest.fixture(scope="function", autouse=True)
def cli_arguments():
    with mock.patch(
        "ska_sdp_pipelines.framework.command.CLICommand"
    ) as cli_arguments_mock:
        yield cli_arguments_mock


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
def write_yml_mock():
    with mock.patch(
        "ska_sdp_pipelines.framework.pipeline.ConfigManager.write_yml"
    ) as write:
        yield write


@pytest.fixture(scope="function", autouse=True)
def create_output_mock():
    with mock.patch(
        "ska_sdp_pipelines.framework.pipeline.create_output_dir",
        return_value="./output/timestamp",
    ) as create_output:
        yield create_output


def test_should_initialise_the_pipeline_with_default_cli_args(cli_arguments):
    cli_arg_mock = Mock(name="cli_arg")
    cli_arguments.return_value = cli_arg_mock
    pipeline = Pipeline("test_pipeline")
    cli_arguments.assert_called_once()
    cli_arg_mock.create_sub_parser.assert_has_calls(
        [
            mock.call(
                "run",
                pipeline._run,
                MANDATORY_CLI_ARGS,
                help="Run the pipeline",
            ),
            mock.call(
                "install-config",
                pipeline._install_config,
                CONFIG_CLI_ARGS,
                help="Installs the default config at --config-install-path",
            ),
        ]
    )


def test_should_initialise_the_pipeline_with_additional_cli_args(
    cli_arguments,
):
    cli_arg_mock = Mock(name="cli_arg")
    cli_arguments.return_value = cli_arg_mock
    cli_args = [CLIArgument("additional_arg1"), CLIArgument("additional_arg2")]
    pipeline = Pipeline("test_pipeline", cli_args=cli_args)

    cli_arg_mock.create_sub_parser.assert_has_calls(
        [
            mock.call(
                "run",
                pipeline._run,
                MANDATORY_CLI_ARGS + cli_args,
                help="Run the pipeline",
            ),
            mock.call(
                "install-config",
                pipeline._install_config,
                CONFIG_CLI_ARGS,
                help="Installs the default config at --config-install-path",
            ),
        ]
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_run_the_pipeline_as_cli_command(
    config_manager_mock,
    read_mock,
    write_mock,
    create_output_mock,
    scheduler_factory,
    default_scheduler,
    cli_arguments,
):
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage2"]
    config_manager_mock.stage_config.side_effect = [
        "stage_config1",
        "stage_config2",
    ]
    config_manager_mock.global_parameters = {"a": 10}

    cli_arguments.return_value = cli_arguments
    cli_arguments.cli_args_dict = {"input": "infile_path"}
    args = Mock(name="CLI_args")
    args.input = "infile_path"
    args.dask_scheduler = None
    args.config_path = None
    args.verbose = False
    args.output_path = None
    args.stages = None

    cli_arguments.parse_args.return_value = args

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {}

    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.config = {}

    global_config = Mock(name="global_configuration")
    global_config.items = {"a": 10}

    pipeline = Pipeline(
        "test_pipeline", stages=[stage1, stage2], global_config=global_config
    )
    args.sub_command = pipeline._run

    pipeline()

    config_manager_mock.assert_called_once_with(
        pipeline={"stage1": True, "stage2": True},
        parameters={},
        global_parameters={"a": 10},
    )

    read_mock.assert_called_once_with("infile_path")
    scheduler_factory.get_scheduler.assert_called_once_with(None)
    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage2],
        verbose=False,
    )

    create_output_mock.assert_called_once_with("./output", "test_pipeline")
    write_mock.assert_called_once_with("output", "./output/timestamp")

    stage1.update_pipeline_parameters.assert_called_once_with(
        "stage_config1",
        _input_data_="dataset",
        _output_dir_="./output/timestamp",
        _cli_args_={"input": "infile_path"},
        _global_parameters_={"a": 10},
    )

    stage2.update_pipeline_parameters.assert_called_once_with(
        "stage_config2",
        _input_data_="dataset",
        _output_dir_="./output/timestamp",
        _cli_args_={"input": "infile_path"},
        _global_parameters_={"a": 10},
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
@mock.patch("ska_sdp_pipelines.framework.pipeline.Configuration")
def test_should_run_the_pipeline(
    configuration_mock,
    config_manager_mock,
    read_mock,
    write_mock,
    create_output_mock,
    scheduler_factory,
    default_scheduler,
):
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage2"]
    config_manager_mock.stage_config.side_effect = [
        "stage_config1",
        "stage_config2",
    ]
    config_manager_mock.global_parameters = {"a": 10}

    configuration_mock.return_value = configuration_mock
    configuration_mock.items = {"a": 10}

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {}
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.config = {}

    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2])

    pipeline.run("infile_path", [])

    config_manager_mock.assert_called_once_with(
        pipeline={"stage1": True, "stage2": True},
        parameters={},
        global_parameters={"a": 10},
    )
    read_mock.assert_called_once_with("infile_path")
    scheduler_factory.get_scheduler.assert_called_once_with(None)
    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage2],
        verbose=False,
    )
    create_output_mock.assert_called_once_with("./output", "test_pipeline")
    write_mock.assert_called_once_with("output", "./output/timestamp")

    stage1.update_pipeline_parameters.assert_called_once_with(
        "stage_config1",
        _input_data_="dataset",
        _output_dir_="./output/timestamp",
        _cli_args_=None,
        _global_parameters_={"a": 10},
    )

    stage2.update_pipeline_parameters.assert_called_once_with(
        "stage_config2",
        _input_data_="dataset",
        _output_dir_="./output/timestamp",
        _cli_args_=None,
        _global_parameters_={"a": 10},
    )


def test_should_run_the_pipeline_with_verbose(log_util):
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {}

    pipeline = Pipeline("test_pipeline", stages=[stage1])
    pipeline.run("infile_path", [], verbose=True)

    log_util.configure.assert_has_calls(
        [
            mock.call("test_pipeline"),
            mock.call(
                "test_pipeline", output_dir="./output/timestamp", verbose=True
            ),
        ]
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_run_the_pipeline_with_selected_stages(
    config_manager_mock, default_scheduler, read_mock
):
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage3"]

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {}
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.config = {}
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.config = {}

    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])
    pipeline.run("infile_path", stages=["stage1", "stage3"])

    config_manager_mock.assert_called_once_with(
        pipeline={"stage1": True, "stage2": True, "stage3": True},
        parameters={},
        global_parameters={},
    )

    config_manager_mock.update_pipeline.assert_called_once_with(
        {"stage1": True, "stage2": False, "stage3": True}
    )

    read_mock.assert_called_once_with("infile_path")
    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage3],
        verbose=False,
    )


def test_should_instantiate_dask_client(scheduler_factory):
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {}
    stage1.stage_config = Configuration()
    pipeline = Pipeline("test_pipeline", stages=[stage1])
    dask_scheduler_address = "some_ip"
    pipeline.run("infile_path", dask_scheduler=dask_scheduler_address)
    scheduler_factory.get_scheduler.assert_called_once_with(
        dask_scheduler_address
    )


def test_should_not_run_if_no_stages_are_provided():
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {}
    pipeline = Pipeline("test_pipeline")
    dask_scheduler_address = "some_ip"
    with pytest.raises(NoStageToExecuteException):
        pipeline.run("infile_path", dask_scheduler=dask_scheduler_address)


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_run_the_pipeline_with_selected_stages_from_config(
    config_manager_mock, default_scheduler, create_output_mock
):
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage3"]
    create_output_mock.return_value = "/path/to/output/timestamp"

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"

    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"

    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"

    stage1.config = {}
    stage2.config = {}
    stage3.config = {}
    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline.run(
        "infile_path",
        config_path="/path/to/config",
        output_path="/path/to/output",
    )

    create_output_mock.assert_called_once_with(
        "/path/to/output", "test_pipeline"
    )

    config_manager_mock.update_config.assert_called_once_with(
        config_path="/path/to/config"
    )

    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage3],
        verbose=False,
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_run_the_pipeline_with_stages_from_cli_over_config(
    config_manager_mock, default_scheduler
):
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage2"]

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"

    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"

    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"

    stage1.config = {}
    stage2.config = {}
    stage3.config = {}

    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline.run(
        "infile_path", ["stage1", "stage2"], config_path="/path/to/config"
    )

    config_manager_mock.update_config.assert_called_once_with(
        config_path="/path/to/config",
    )
    config_manager_mock.update_pipeline.assert_called_once_with(
        {"stage1": True, "stage2": True, "stage3": False},
    )

    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage2],
        verbose=False,
    )


def test_should_raise_exception_if_wrong_stage_is_provided():
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"

    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"

    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"

    stage1.config = {}
    stage2.config = {}
    stage3.config = {}

    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    with pytest.raises(StageNotFoundException):
        pipeline.run("infile_path", ["stage1", "stage5"])


def test_should_return_pipeline_default_configuration():
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {"stage1": "stage1_config"}

    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.config = {"stage2": "stage2_config"}

    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.config = {"stage3": "stage3_config"}

    global_config = Mock(name="global_configuration")
    global_config.items = {"global_param1": "value1"}

    pipeline = Pipeline(
        "test_pipeline",
        stages=[stage1, stage2, stage3],
        global_config=global_config,
    )

    expected_config = {
        "pipeline": {"stage1": True, "stage2": True, "stage3": True},
        "parameters": {
            "stage1": "stage1_config",
            "stage2": "stage2_config",
            "stage3": "stage3_config",
        },
        "global_parameters": {"global_param1": "value1"},
    }

    assert pipeline.config == expected_config


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_install_default_config(config_manager_mock):
    config_manager_mock.return_value = config_manager_mock
    args_mock = Mock(name="arg_mock")
    args_mock.config_install_path = "/path/to/install"
    pipeline = Pipeline("test_pipeline")
    pipeline._install_config(args_mock)

    config_manager_mock.write_yml.assert_called_once_with(
        "/path/to/install/test_pipeline.yml"
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_write_config_to_output_yaml_file(
    config_manager_mock, default_scheduler
):
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1"]

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {"stage1": "stage1_config"}

    pipeline = Pipeline("test", stages=[stage1])

    pipeline.run("infile", output_path="/output")

    config_manager_mock.write_yml.assert_called_once_with(
        "./output/timestamp/config.yml"
    )


@mock.patch("ska_sdp_pipelines.framework.pipeline.ConfigManager")
def test_should_write_config_to_output_yaml_on_failure(
    config_manager_mock, default_scheduler
):
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1"]

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {"stage1": "stage1_config"}

    manager = Mock()
    manager.attach_mock(config_manager_mock.write_yml, "mocked_write_yml")
    manager.attach_mock(default_scheduler.execute, "execute_mock")

    pipeline = Pipeline("test", stages=[stage1])

    pipeline.run("infile", output_path="/output")

    manager.assert_has_calls(
        [
            call.mocked_write_yml("./output/timestamp/config.yml"),
            call.execute_mock(),
        ]
    )


def test_should_get_instance_of_pipeline():
    pipeline = Pipeline("test_pipeline", ["stage"])
    assert pipeline == Pipeline("test_pipeline", _existing_instance_=True)
