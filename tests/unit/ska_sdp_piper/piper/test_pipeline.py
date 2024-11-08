import pytest
from mock import Mock, mock

from ska_sdp_piper.piper.command.cli_command_parser import CLIArgument
from ska_sdp_piper.piper.configurations import Configuration
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
    ) as cli_arguments_mock:
        yield cli_arguments_mock


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


def test_should_initialise_the_pipeline_with_default_cli_args(
    cli_command_parser, stages, default_scheduler
):
    cli_arg_mock = Mock(name="cli_arg")
    cli_command_parser.return_value = cli_arg_mock
    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )
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
    cli_command_parser, stages, default_scheduler
):
    cli_arg_mock = Mock(name="cli_arg")
    cli_command_parser.return_value = cli_arg_mock
    cli_args = [CLIArgument("additional_arg1"), CLIArgument("additional_arg2")]
    pipeline = Pipeline(
        "test_pipeline",
        cli_args=cli_args,
        stages=stages,
        scheduler=default_scheduler,
    )

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


@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_run_the_pipeline_from_cli_command(
    runtime_config_mock,
    create_output_mock,
    cli_command_parser,
    timestamp_mock,
    stages,
    default_scheduler,
):
    runtime_config_mock.update_from_cli_stages.return_value = runtime_config_mock
    runtime_config_mock.update_from_cli_overrides.return_value = runtime_config_mock
    runtime_config_mock.update_from_yaml.return_value = runtime_config_mock
    runtime_config_mock.return_value = runtime_config_mock
    runtime_config_mock.stages_to_run = ["a", "b"]

    cli_command_parser.return_value = cli_command_parser
    cli_command_parser.cli_args_dict = {"input": "infile_path"}

    args = Mock(name="CLI_args")
    args.input = "infile_path"
    args.dask_scheduler = "10.131"
    args.config_path = "config_path"
    args.override_defaults = "CLI_OVERRIDES"
    args.verbose = False
    args.output_path = "output_path_from_cli"
    args.stages = [["a", "b"]]

    cli_command_parser.parse_args.return_value = args
    pipeline_run_mock = Mock(name="pipeline_run_mock")

    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )
    args.sub_command = pipeline._run
    pipeline.run = pipeline_run_mock

    pipeline()

    runtime_config_mock.update_from_yaml.assert_called_once_with("config_path")
    runtime_config_mock.update_from_cli_overrides.assert_called_once_with("CLI_OVERRIDES")
    runtime_config_mock.update_from_cli_stages.assert_called_once_with(["a", "b"])

    create_output_mock.assert_called_once_with(
        "output_path_from_cli", "test_pipeline"
    )
    cli_command_parser.write_yml.assert_called_once_with(
        "./output/timestamp/test_pipeline_FORMATTED_TIME.cli.yml"
    )

    runtime_config_mock.write_yml.assert_called_once_with(
        "./output/timestamp/test_pipeline_FORMATTED_TIME.config.yml",
    )

    pipeline_run_mock.assert_called_once_with(
        stages=["a", "b"],
        verbose=False,
        output_dir="./output/timestamp",
        cli_args={"input": "infile_path"},
    )


@mock.patch("ska_sdp_piper.piper.pipeline.timestamp")
@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
@mock.patch("ska_sdp_piper.piper.pipeline.Configuration")
def test_should_run_the_pipeline(
    configuration_mock,
    runtime_config_mock,
    timestamp_mock,
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

    runtime_config_mock.return_value = runtime_config_mock
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

    pipeline.run(
        "output_dir",
        [],
        cli_args={"input": "path", "dask_scheduler": "10.191"},
    )

    executor_factory.get_executor.assert_called_once_with(
        "output_dir",
        input="path",
        dask_scheduler="10.191",
    )

    runtime_config_mock.assert_called_once_with(
        pipeline={"stage1": True, "stage2": True, "stage3": True},
        parameters={
            "stage1": "stage1_config",
            "stage2": "stage2_config",
            "stage3": "stage3_config",
        },
        global_parameters={"a": 10},
    )

    runtime_config_mock.write_yml.assert_called_once_with(
        "output_dir/test_pipeline_FORMATTED_TIME.config.yml"
    )

    default_scheduler.schedule.assert_called_once_with(mock_stages)

    default_executor.execute.assert_called_once_with(default_scheduler.tasks)

    stages.add_additional_parameters.assert_called_once_with(
        _output_dir_="output_dir",
        _cli_args_={"input": "path", "dask_scheduler": "10.191"},
        _global_parameters_={"a": 10},
    )


@mock.patch("ska_sdp_piper.piper.pipeline.timestamp")
def test_should_run_the_pipeline_with_verbose(
    timestamp_mock, log_util, stages, default_scheduler
):
    timestamp_mock.return_value = "FORMATTED_TIME"
    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )
    pipeline.run("output_dir", [], verbose=True)

    log_util.configure.assert_has_calls(
        [
            mock.call(
                "output_dir/test_pipeline_FORMATTED_TIME.log", verbose=True
            ),
        ]
    )


@mock.patch("ska_sdp_piper.piper.pipeline.timestamp")
@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_run_the_pipeline_with_selected_stages(
    runtime_config_mock, timestamp_mock, default_scheduler, mock_stages
):
    timestamp_mock.return_value = "FORMATTED_TIME"
    stage1, _, stage3 = mock_stages
    runtime_config_mock.return_value = runtime_config_mock
    runtime_config_mock.stages_to_run = ["stage1", "stage3"]

    stages = Stages(mock_stages)
    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )
    pipeline.run("output_dir", stages=["stage1", "stage3"])

    runtime_config_mock.assert_called_once_with(
        pipeline={"stage1": True, "stage2": True, "stage3": True},
        parameters={},
        global_parameters={},
    )

    runtime_config_mock.update_pipeline.assert_called_once_with(
        {"stage1": True, "stage2": False, "stage3": True}
    )

    default_scheduler.schedule.assert_called_once_with([stage1, stage3])


@mock.patch("ska_sdp_piper.piper.pipeline.timestamp")
def test_should_instantiate_dask_client(
    timestamp_mock, executor_factory, default_scheduler
):
    timestamp_mock.return_value = "FORMATTED_TIME"
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {}
    stage1.stage_config = Configuration()
    stages = Stages([stage1])
    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )
    dask_scheduler_address = "some_ip"
    pipeline.run(
        "output_dir",
        cli_args={"dask_scheduler": dask_scheduler_address},
    )
    executor_factory.get_executor.assert_called_once_with(
        "output_dir",
        dask_scheduler=dask_scheduler_address,
    )


@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_update_the_default_stage_configs_if_external_yaml_is_provided(
    runtime_config_mock, default_scheduler, create_output_mock, stages
):

    runtime_config_mock.return_value = runtime_config_mock
    runtime_config_mock.stages_to_run = ["stage1", "stage3"]
    create_output_mock.return_value = "/path/to/output/timestamp"
    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )

    pipeline.run("/path/to/output", config_path="/path/to/config")

    stages.update_stage_parameters.assert_called_once_with(
        runtime_config_mock.parameters
    )


@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_run_the_pipeline_with_selected_stages_from_config(
    runtime_config_mock, default_scheduler, create_output_mock, mock_stages
):
    stage1, _, stage3 = mock_stages
    runtime_config_mock.return_value = runtime_config_mock
    runtime_config_mock.stages_to_run = ["stage1", "stage3"]
    create_output_mock.return_value = "/path/to/output/timestamp"
    stages = Stages(mock_stages)
    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )

    pipeline.run("/path/to/output", config_path="/path/to/config")

    runtime_config_mock.update_config.assert_called_once_with(
        "/path/to/config"
    )

    default_scheduler.schedule.assert_called_once_with([stage1, stage3])


@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_run_the_pipeline_with_stages_from_cli_over_config(
    runtime_config_mock, default_scheduler, mock_stages
):
    stage1, stage2, _ = mock_stages
    runtime_config_mock.return_value = runtime_config_mock
    runtime_config_mock.stages_to_run = ["stage1", "stage2"]

    stages = Stages(mock_stages)
    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )

    pipeline.run(
        "output_dir",
        ["stage1", "stage2"],
        config_path="/path/to/config",
    )

    runtime_config_mock.update_config.assert_called_once_with(
        "/path/to/config",
    )
    runtime_config_mock.update_pipeline.assert_called_once_with(
        {"stage1": True, "stage2": True, "stage3": False},
    )

    default_scheduler.schedule.assert_called_once_with([stage1, stage2])


@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_not_update_config_if_config_path_is_not_provided(
    runtime_config_mock, default_scheduler
):
    runtime_config_mock.return_value = runtime_config_mock

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"

    stage1.config = {}
    stages = Stages([stage1])

    pipeline = Pipeline(
        "test_pipeline", stages=stages, scheduler=default_scheduler
    )

    pipeline.run("output_dir")

    assert runtime_config_mock.update_config.call_count == 0


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
    args_mock = Mock(name="arg_mock")
    args_mock.config_install_path = "/path/to/install"
    args_mock.overide_defaults = None
    pipeline = Pipeline(
        "test_pipeline", stages=Stages(), scheduler=default_scheduler
    )
    pipeline._install_config(args_mock)

    runtime_config_mock.write_yml.assert_called_once_with(
        "/path/to/install/test_pipeline.yml"
    )


@mock.patch("ska_sdp_piper.piper.pipeline.yaml")
@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_override_install_default_config(
    runtime_config_mock, yaml_mock, default_scheduler
):
    runtime_config_mock.return_value = runtime_config_mock
    args_mock = Mock(name="arg_mock")
    args_mock.config_install_path = "/path/to/install"
    args_mock.overide_defaults = [
        ["parameters.b", "2"],
        ["parameters.y.z", "[oea, aoe, xyz]"],
    ]
    yaml_mock.safe_load.return_value = {
        "parameters.b": 2,
        "parameters.y.z": ["oea", "aoe", "xyz"],
    }

    pipeline = Pipeline(
        "test_pipeline", stages=Stages(), scheduler=default_scheduler
    )
    pipeline._install_config(args_mock)

    yaml_mock.safe_load.assert_called_once_with(
        "parameters.b : 2\nparameters.y.z : [oea, aoe, xyz]"
    )

    runtime_config_mock.set.assert_has_calls(
        [
            mock.call("parameters.b", 2),
            mock.call("parameters.y.z", ["oea", "aoe", "xyz"]),
        ]
    )

    runtime_config_mock.write_yml.assert_called_once_with(
        "/path/to/install/test_pipeline.yml"
    )


@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_write_config_to_output_yaml_file(
    runtime_config_mock, timestamp_mock, default_scheduler
):
    runtime_config_mock.return_value = runtime_config_mock
    runtime_config_mock.stages_to_run = ["stage1"]

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {"stage1": "stage1_config"}

    pipeline = Pipeline(
        "test", stages=Stages([stage1]), scheduler=default_scheduler
    )

    pipeline.run("output_dir")

    runtime_config_mock.write_yml.assert_called_once_with(
        "output_dir/test_FORMATTED_TIME.config.yml"
    )


@mock.patch("ska_sdp_piper.piper.pipeline.RuntimeConfig")
def test_should_write_config_to_output_yaml_on_failure(
    runtime_config_mock, default_scheduler, default_executor, timestamp_mock
):
    runtime_config_mock.return_value = runtime_config_mock
    runtime_config_mock.stages_to_run = ["stage1"]
    default_executor.execute.side_effect = Exception("Some Error")

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.config = {"stage1": "stage1_config"}

    pipeline = Pipeline(
        "test", stages=Stages([stage1]), scheduler=default_scheduler
    )

    with pytest.raises(Exception):
        pipeline.run("output_dir")

    runtime_config_mock.write_yml.assert_called_once_with(
        "output_dir/test_FORMATTED_TIME.config.yml"
    )


def test_should_get_instance_of_pipeline(default_scheduler):
    pipeline = Pipeline(
        "test_pipeline", Stages(["stage"]), scheduler=default_scheduler
    )
    assert pipeline == Pipeline("test_pipeline", _existing_instance_=True)
