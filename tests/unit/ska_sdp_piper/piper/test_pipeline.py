import pytest
from mock import Mock, mock

from ska_sdp_piper.piper.command.cli_command_parser import CLIArgument
from ska_sdp_piper.piper.configurations import Configuration
from ska_sdp_piper.piper.constants import CONFIG_CLI_ARGS, DEFAULT_CLI_ARGS
from ska_sdp_piper.piper.pipeline import Pipeline


@pytest.fixture(scope="function", autouse=True)
def log_util():
    with mock.patch("ska_sdp_piper.piper.pipeline.LogUtil") as log_util_mock:
        yield log_util_mock


@pytest.fixture(scope="function")
def mock_stages():
    stage1 = Mock(name="stage1")
    stage1.name = "stage1"
    stage1.config = {}

    stage2 = Mock(name="stage2")
    stage2.name = "stage2"
    stage2.config = {}

    stage3 = Mock(name="stage3")
    stage3.name = "stage3"
    stage3.config = {}

    yield [stage1, stage2, stage3]


@pytest.fixture(scope="function")
def stages():
    with mock.patch("ska_sdp_piper.piper.pipeline.Stages") as stages_mock:
        stages_mock.return_value = stages_mock
        yield stages_mock


@pytest.fixture(scope="function", autouse=True)
def cli_command_parser():
    with mock.patch(
        "ska_sdp_piper.piper.command.command.CLICommandParser"
    ) as cli_arguments_mock:
        yield cli_arguments_mock


@pytest.fixture(scope="function")
def default_scheduler():
    with mock.patch(
        "ska_sdp_piper.piper.scheduler.DefaultScheduler"
    ) as default_scheduler_mock:

        default_scheduler_mock.execute.return_value = "output"

        yield default_scheduler_mock


@pytest.fixture(scope="function", autouse=True)
def scheduler_factory(default_scheduler):
    with mock.patch(
        "ska_sdp_piper.piper.pipeline.SchedulerFactory"
    ) as scheduler_factory_mock:
        scheduler_factory_mock.get_scheduler.return_value = default_scheduler
        yield scheduler_factory_mock


@pytest.fixture(scope="function", autouse=True)
def read_mock():
    with mock.patch(
        "ska_sdp_piper.piper.pipeline.read_dataset",
        return_value="dataset",
    ) as read:
        yield read


@pytest.fixture(scope="function", autouse=True)
def write_mock():
    with mock.patch("ska_sdp_piper.piper.pipeline.write_dataset") as write:
        yield write


@pytest.fixture(scope="function", autouse=True)
def write_yml_mock():
    with mock.patch(
        "ska_sdp_piper.piper.pipeline.ConfigManager.write_yml"
    ) as write:
        yield write


@pytest.fixture(scope="function", autouse=True)
def create_output_mock():
    with mock.patch(
        "ska_sdp_piper.piper.pipeline.create_output_dir",
        return_value="./output/timestamp",
    ) as create_output:
        yield create_output


@pytest.fixture(scope="function")
def timestamp_mock():
    with mock.patch("ska_sdp_piper.piper.pipeline.timestamp") as timestamp:
        timestamp.return_value = "FORMATTED_TIME"

        yield timestamp


def test_should_initialise_the_pipeline_with_default_cli_args(
    cli_command_parser,
):
    cli_arg_mock = Mock(name="cli_arg")
    cli_command_parser.return_value = cli_arg_mock
    pipeline = Pipeline("test_pipeline")
    cli_command_parser.assert_called_once()
    cli_arg_mock.create_sub_parser.assert_has_calls(
        [
            mock.call(
                "run",
                pipeline._run,
                DEFAULT_CLI_ARGS,
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
    cli_command_parser,
):
    cli_arg_mock = Mock(name="cli_arg")
    cli_command_parser.return_value = cli_arg_mock
    cli_args = [CLIArgument("additional_arg1"), CLIArgument("additional_arg2")]
    pipeline = Pipeline("test_pipeline", cli_args=cli_args)

    cli_arg_mock.create_sub_parser.assert_has_calls(
        [
            mock.call(
                "run",
                pipeline._run,
                DEFAULT_CLI_ARGS + cli_args,
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


def test_should_run_the_pipeline_from_cli_command(
    create_output_mock, cli_command_parser, timestamp_mock
):
    cli_command_parser.return_value = cli_command_parser
    cli_command_parser.cli_args_dict = {"input": "infile_path"}
    args = Mock(name="CLI_args")
    args.input = "infile_path"
    args.dask_scheduler = "10.131"
    args.config_path = "config_path"
    args.verbose = False
    args.output_path = "output_path_from_cli"
    args.stages = [["a", "b"]]

    cli_command_parser.parse_args.return_value = args
    pipeline_run_mock = Mock(name="pipeline_run_mock")

    pipeline = Pipeline("test_pipeline", stages=[])
    args.sub_command = pipeline._run
    pipeline.run = pipeline_run_mock

    pipeline()

    create_output_mock.assert_called_once_with(
        "output_path_from_cli", "test_pipeline"
    )
    cli_command_parser.write_yml.assert_called_once_with(
        "./output/timestamp/test_pipeline_FORMATTED_TIME.cli.yml"
    )
    pipeline_run_mock.assert_called_once_with(
        "infile_path",
        stages=["a", "b"],
        dask_scheduler="10.131",
        config_path="config_path",
        verbose=False,
        output_dir="./output/timestamp",
        cli_args={"input": "infile_path"},
    )


def test_should_run_the_pipeline_from_cli_command_with_default_output(
    create_output_mock, cli_command_parser, timestamp_mock
):
    cli_command_parser.return_value = cli_command_parser
    args = Mock(name="CLI_args")
    args.output_path = None
    args.stages = None
    args.verbose = 1

    cli_command_parser.parse_args.return_value = args
    pipeline_run_mock = Mock(name="pipeline_run_mock")

    pipeline = Pipeline("test_pipeline", stages=[])
    args.sub_command = pipeline._run
    pipeline.run = pipeline_run_mock

    pipeline()

    create_output_mock.assert_called_once_with("./output", "test_pipeline")
    cli_command_parser.write_yml.assert_called_once_with(
        "./output/timestamp/test_pipeline_FORMATTED_TIME.cli.yml"
    )
    pipeline_run_mock.assert_called_once_with(
        args.input,
        stages=[],
        dask_scheduler=args.dask_scheduler,
        config_path=args.config_path,
        verbose=True,
        output_dir="./output/timestamp",
        cli_args=cli_command_parser.cli_args_dict,
    )


@mock.patch("ska_sdp_piper.piper.pipeline.ConfigManager")
@mock.patch("ska_sdp_piper.piper.pipeline.Configuration")
def test_should_run_the_pipeline(
    configuration_mock,
    config_manager_mock,
    read_mock,
    write_mock,
    create_output_mock,
    scheduler_factory,
    default_scheduler,
    mock_stages,
    stages,
):
    stages.__iter__.return_value = mock_stages
    stages.get_stages.return_value = mock_stages
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage2"]
    config_manager_mock.stage_config.side_effect = [
        "stage_config1",
        "stage_config2",
    ]
    config_manager_mock.global_parameters = {"a": 10}

    configuration_mock.return_value = configuration_mock
    configuration_mock.items = {"a": 10}

    pipeline = Pipeline("test_pipeline", stages=mock_stages)

    pipeline.run(
        "infile_path",
        "output_dir",
        [],
        dask_scheduler="10.191",
        cli_args={"input": "path"},
    )

    config_manager_mock.assert_called_once_with(
        pipeline={"stage1": True, "stage2": True, "stage3": True},
        parameters={},
        global_parameters={"a": 10},
    )
    read_mock.assert_called_once_with("infile_path")
    scheduler_factory.get_scheduler.assert_called_once_with("10.191")
    default_scheduler.schedule.assert_called_once_with(
        mock_stages,
        verbose=False,
    )

    write_mock.assert_called_once_with("output", "output_dir")

    stages.update_pipeline_parameters.assert_called_once_with(
        config_manager_mock.stages_to_run,
        config_manager_mock.parameters,
        _input_data_="dataset",
        _output_dir_="output_dir",
        _cli_args_={"input": "path"},
        _global_parameters_={"a": 10},
    )


def test_should_run_the_pipeline_with_verbose(log_util):
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {}

    pipeline = Pipeline("test_pipeline", stages=[stage1])
    pipeline.run("infile_path", "output_dir", [], verbose=True)

    log_util.configure.assert_has_calls(
        [
            mock.call("test_pipeline"),
            mock.call("test_pipeline", output_dir="output_dir", verbose=True),
        ]
    )


@mock.patch("ska_sdp_piper.piper.pipeline.ConfigManager")
def test_should_run_the_pipeline_with_selected_stages(
    config_manager_mock, default_scheduler, read_mock, mock_stages
):
    stage1, _, stage3 = mock_stages
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage3"]

    pipeline = Pipeline("test_pipeline", stages=mock_stages)
    pipeline.run("infile_path", "output_dir", stages=["stage1", "stage3"])

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
    pipeline.run(
        "infile_path", "output_dir", dask_scheduler=dask_scheduler_address
    )
    scheduler_factory.get_scheduler.assert_called_once_with(
        dask_scheduler_address
    )


@mock.patch("ska_sdp_piper.piper.pipeline.ConfigManager")
def test_should_run_the_pipeline_with_selected_stages_from_config(
    config_manager_mock, default_scheduler, create_output_mock, mock_stages
):
    stage1, stage2, stage3 = mock_stages
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage3"]
    create_output_mock.return_value = "/path/to/output/timestamp"

    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline.run(
        "infile_path",
        "/path/to/output",
        config_path="/path/to/config",
    )

    config_manager_mock.update_config.assert_called_once_with(
        "/path/to/config"
    )

    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage3],
        verbose=False,
    )


@mock.patch("ska_sdp_piper.piper.pipeline.ConfigManager")
def test_should_run_the_pipeline_with_stages_from_cli_over_config(
    config_manager_mock, default_scheduler, mock_stages
):
    stage1, stage2, stage3 = mock_stages
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1", "stage2"]

    pipeline = Pipeline("test_pipeline", stages=[stage1, stage2, stage3])

    pipeline.run(
        "infile_path",
        "output_dir",
        ["stage1", "stage2"],
        config_path="/path/to/config",
    )

    config_manager_mock.update_config.assert_called_once_with(
        "/path/to/config",
    )
    config_manager_mock.update_pipeline.assert_called_once_with(
        {"stage1": True, "stage2": True, "stage3": False},
    )

    default_scheduler.schedule.assert_called_once_with(
        [stage1, stage2],
        verbose=False,
    )


@mock.patch("ska_sdp_piper.piper.pipeline.ConfigManager")
def test_should_not_update_config_if_config_path_is_not_provided(
    config_manager_mock, default_scheduler
):
    config_manager_mock.return_value = config_manager_mock

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"

    stage1.config = {}

    pipeline = Pipeline("test_pipeline", stages=[stage1])

    pipeline.run("infile_path", "output_dir")

    assert config_manager_mock.update_config.call_count == 0


@mock.patch("ska_sdp_piper.piper.pipeline.ConfigManager")
def test_should_return_pipeline_default_configuration(config_manager_mock):

    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.config = {"config": "config"}

    pipeline = Pipeline("test_pipeline")

    assert pipeline.config == {"config": "config"}


@mock.patch("ska_sdp_piper.piper.pipeline.ConfigManager")
def test_should_install_default_config(config_manager_mock):
    config_manager_mock.return_value = config_manager_mock
    args_mock = Mock(name="arg_mock")
    args_mock.config_install_path = "/path/to/install"
    pipeline = Pipeline("test_pipeline")
    pipeline._install_config(args_mock)

    config_manager_mock.write_yml.assert_called_once_with(
        "/path/to/install/test_pipeline.yml"
    )


@mock.patch("ska_sdp_piper.piper.pipeline.ConfigManager")
def test_should_write_config_to_output_yaml_file(
    config_manager_mock, timestamp_mock
):
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1"]

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {"stage1": "stage1_config"}

    pipeline = Pipeline("test", stages=[stage1])

    pipeline.run("infile", "output_dir")

    config_manager_mock.write_yml.assert_called_once_with(
        "output_dir/test_FORMATTED_TIME.config.yml"
    )


@mock.patch("ska_sdp_piper.piper.pipeline.ConfigManager")
def test_should_write_config_to_output_yaml_on_failure(
    config_manager_mock, default_scheduler, timestamp_mock
):
    config_manager_mock.return_value = config_manager_mock
    config_manager_mock.stages_to_run = ["stage1"]
    default_scheduler.execute.side_effect = Exception("Some Error")

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {"stage1": "stage1_config"}

    pipeline = Pipeline("test", stages=[stage1])

    with pytest.raises(Exception):
        pipeline.run("infile", "output_dir")

    config_manager_mock.write_yml.assert_called_once_with(
        "output_dir/test_FORMATTED_TIME.config.yml"
    )


def test_should_get_instance_of_pipeline():
    pipeline = Pipeline("test_pipeline", ["stage"])
    assert pipeline == Pipeline("test_pipeline", _existing_instance_=True)
