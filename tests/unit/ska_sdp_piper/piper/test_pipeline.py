import pytest
from mock import Mock, mock

from ska_sdp_piper.piper.command.cli_command_parser import CLIArgument
from ska_sdp_piper.piper.constants import CONFIG_CLI_ARGS, DEFAULT_CLI_ARGS
from ska_sdp_piper.piper.pipeline import Pipeline
from ska_sdp_piper.piper.stage import Stages


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
def stages(mock_stages):
    with mock.patch("ska_sdp_piper.piper.stage.Stages") as stages_mock:
        stages_mock.return_value = stages_mock
        stages_mock.__iter__.return_value = mock_stages
        stages_mock.get_stages.return_value = mock_stages

        yield stages_mock


@pytest.fixture(scope="function", autouse=True)
def cli_command_parser():
    with mock.patch(
        "ska_sdp_piper.piper.command.command.CLICommandParser"
    ) as cli_command_parser_mock:
        cli_command_parser_mock.return_value = cli_command_parser_mock
        yield cli_command_parser_mock


@pytest.fixture(scope="function")
def default_scheduler():
    with mock.patch(
        "ska_sdp_piper.piper.scheduler.PiperScheduler"
    ) as piper_scheduler_mock:
        yield piper_scheduler_mock


@pytest.fixture(scope="function")
def default_executor():
    with mock.patch(
        "ska_sdp_piper.piper.executors.default_executor.DefaultExecutor"
    ) as default_executor_mock:

        default_executor_mock.execute.return_value = "output"

        yield default_executor_mock


@pytest.fixture(scope="function", autouse=True)
def executor_factory(default_executor):
    with mock.patch(
        "ska_sdp_piper.piper.pipeline.ExecutorFactory"
    ) as executor_factory_mock:
        executor_factory_mock.get_executor.return_value = default_executor
        yield executor_factory_mock


@pytest.fixture(scope="function", autouse=True)
def write_yml_mock():
    with mock.patch(
        "ska_sdp_piper.piper.pipeline.RuntimeConfig.write_yml"
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


@pytest.fixture(scope="function")
def runtime_config_mock():
    with mock.patch(
        "ska_sdp_piper.piper.pipeline.RuntimeConfig"
    ) as run_config:
        run_config.update_from_cli_stages.return_value = run_config
        run_config.update_from_cli_overrides.return_value = run_config
        run_config.update_from_yaml.return_value = run_config
        run_config.return_value = run_config

        yield run_config


def test_should_initialise_the_pipeline_with_default_cli_args(
    cli_command_parser, stages, default_scheduler
):
    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )
    cli_command_parser.assert_called_once()
    cli_command_parser.create_sub_parser.assert_has_calls(
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
    cli_command_parser, stages, default_scheduler
):
    cli_args = [CLIArgument("additional_arg1"), CLIArgument("additional_arg2")]
    pipeline = Pipeline(
        "test_pipeline",
        cli_args=cli_args,
        stages=stages,
        scheduler=default_scheduler,
    )

    cli_command_parser.create_sub_parser.assert_has_calls(
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


@mock.patch("ska_sdp_piper.piper.pipeline.Configuration")
def test_should_run_the_pipeline_from_cli_command(
    configuration_mock,
    runtime_config_mock,
    create_output_mock,
    cli_command_parser,
    timestamp_mock,
    log_util,
    stages,
    default_scheduler,
):
    runtime_config_mock.stages_to_run = ["a", "b"]

    args = {}
    args["dask_scheduler"] = "10.131"
    args["config_path"] = "config_path"
    args["override_defaults"] = "CLI_OVERRIDES"
    args["verbose"] = 0
    args["output_path"] = "output_path_from_cli"
    args["stages"] = ["a", "b"]

    cli_command_parser.cli_args_dict = args

    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )
    args["sub_command"] = pipeline._run

    pipeline()

    runtime_config_mock.update_from_yaml.assert_called_once_with("config_path")
    runtime_config_mock.update_from_cli_overrides.assert_called_once_with(
        "CLI_OVERRIDES"
    )
    runtime_config_mock.update_from_cli_stages.assert_called_once_with(
        ["a", "b"]
    )

    create_output_mock.assert_called_once_with(
        "output_path_from_cli", "test_pipeline"
    )

    log_util.configure.assert_has_calls(
        [
            mock.call(
                "./output/timestamp/test_pipeline_FORMATTED_TIME.log",
                verbose=False,
            ),
        ]
    )

    cli_command_parser.write_yml.assert_called_once_with(
        "./output/timestamp/test_pipeline_FORMATTED_TIME.cli.yml"
    )

    runtime_config_mock.write_yml.assert_called_once_with(
        "./output/timestamp/test_pipeline_FORMATTED_TIME.config.yml",
    )

    stages.add_additional_parameters.assert_called_once_with(
        _output_dir_="./output/timestamp",
        _cli_args_={"sub_command": pipeline._run},
        _global_parameters_=configuration_mock.return_value.items,
    )


@mock.patch("ska_sdp_piper.piper.pipeline.timestamp")
@mock.patch("ska_sdp_piper.piper.pipeline.Configuration")
def test_should_run_the_pipeline(
    configuration_mock,
    timestamp_mock,
    runtime_config_mock,
    create_output_mock,
    executor_factory,
    default_scheduler,
    default_executor,
    mock_stages,
    stages,
):
    timestamp_mock.return_value = "FORMATTED_TIME"

    mock_stage1, mock_stage2, mock_stage3 = mock_stages
    mock_stage1.config = {"stage1": "stage1_config"}
    mock_stage2.config = {"stage2": "stage2_config"}
    mock_stage3.config = {"stage3": "stage3_config"}

    runtime_config_mock.stages_to_run = ["stage1", "stage2"]
    runtime_config_mock.stage_config.side_effect = [
        "stage_config1",
        "stage_config2",
    ]
    runtime_config_mock.global_parameters = {"a": 10}

    configuration_mock.return_value = configuration_mock
    configuration_mock.items = {"a": 10}

    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )

    args = {}
    args["dask_scheduler"] = "10.131"
    args["config_path"] = "config_path"
    args["override_defaults"] = "CLI_OVERRIDES"
    args["verbose"] = 0
    args["output_path"] = "output_dir"
    args["stages"] = ["stage1", "stage2"]
    args["with_report"] = True
    args["new_argument"] = 123

    pipeline._run(**args)

    executor_factory.get_executor.assert_called_once_with(
        dask_scheduler="10.131",
        output_dir="./output/timestamp",
        verbose=False,
        with_report=True,
    )

    default_scheduler.schedule.assert_called_once_with(mock_stages)

    default_executor.execute.assert_called_once_with(default_scheduler.tasks)

    stages.validate.assert_called_once_with(["stage1", "stage2"])
    stages.get_stages.assert_called_once_with(["stage1", "stage2"])
    stages.add_additional_parameters.assert_called_once_with(
        _output_dir_="./output/timestamp",
        _cli_args_={"new_argument": 123},
        _global_parameters_={"a": 10},
    )


def test_should_return_pipeline_default_configuration(
    default_scheduler, stages, mock_stages
):
    mock_stage1, mock_stage2, mock_stage3 = mock_stages
    mock_stage1.config = {"stage1": "stage1_config"}
    mock_stage2.config = {"stage2": "stage2_config"}
    mock_stage3.config = {"stage3": "stage3_config"}

    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )

    assert pipeline.config == {
        "global_parameters": {},
        "parameters": {
            "stage1": "stage1_config",
            "stage2": "stage2_config",
            "stage3": "stage3_config",
        },
        "pipeline": {
            "stage1": True,
            "stage2": True,
            "stage3": True,
        },
    }


@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_install_default_config(runtime_config_mock, default_scheduler):
    runtime_config_mock.return_value = runtime_config_mock
    runtime_config_mock.update_from_cli_overrides = runtime_config_mock

    pipeline = Pipeline(
        "test_pipeline", stages=Stages(), scheduler=default_scheduler
    )
    pipeline._install_config(config_install_path="/path/to/install")

    runtime_config_mock.write_yml.assert_called_once_with(
        "/path/to/install/test_pipeline.yml"
    )


@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_override_install_default_config(
    runtime_config_mock, default_scheduler
):
    runtime_config_mock.return_value = runtime_config_mock

    pipeline = Pipeline(
        "test_pipeline", stages=Stages(), scheduler=default_scheduler
    )
    pipeline._install_config(
        config_install_path="/path/to/install",
        override_defaults=[
            ["parameters.b", "2"],
            ["parameters.y.z", "[oea, aoe, xyz]"],
        ],
    )

    runtime_config_mock.update_from_cli_overrides.assert_called_once_with(
        [
            ["parameters.b", "2"],
            ["parameters.y.z", "[oea, aoe, xyz]"],
        ]
    )


def test_should_get_instance_of_pipeline(default_scheduler):
    pipeline = Pipeline(
        "test_pipeline", Stages(["stage"]), scheduler=default_scheduler
    )
    assert pipeline == Pipeline("test_pipeline", _existing_instance_=True)
