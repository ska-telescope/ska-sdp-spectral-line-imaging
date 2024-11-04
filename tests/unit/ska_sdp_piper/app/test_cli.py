import pytest
from mock import Mock, mock

from ska_sdp_piper.app.cli import benchmark, install, uninstall


@mock.patch("ska_sdp_piper.app.cli.ExecutablePipeline")
def test_should_install_executable(exec_pip_mock):
    exec_pip_mock.return_value = exec_pip_mock

    install("pipeline_name", "/path/to/pipeline")

    exec_pip_mock.assert_called_once_with("pipeline_name", "/path/to/pipeline")
    exec_pip_mock.validate_pipeline.assert_called_once()
    exec_pip_mock.prepare_executable.assert_called_once()
    exec_pip_mock.install.assert_called_once()


@mock.patch("ska_sdp_piper.app.cli.ExecutablePipeline")
def test_should_install_executable_and_the_config(exec_pip_mock):
    exec_pip_mock.return_value = exec_pip_mock

    install(
        "pipeline_name",
        "/path/to/pipeline",
        config_install_path="/path/to/config",
    )

    exec_pip_mock.assert_called_once_with("pipeline_name", "/path/to/pipeline")
    exec_pip_mock.validate_pipeline.assert_called_once()
    exec_pip_mock.prepare_executable.assert_called_once()
    exec_pip_mock.install.assert_called_once_with("/path/to/config")


@mock.patch("ska_sdp_piper.app.cli.ExecutablePipeline")
def test_should_uninstall_executable(exec_pip_mock):
    exec_pip_mock.return_value = exec_pip_mock

    uninstall("pipeline_name", "/path/to/pipeline")

    exec_pip_mock.assert_called_once_with("pipeline_name", "/path/to/pipeline")
    exec_pip_mock.validate_pipeline.assert_called_once()
    exec_pip_mock.prepare_executable.assert_called_once()
    exec_pip_mock.uninstall.assert_called_once()


@mock.patch("ska_sdp_piper.app.cli.os")
@mock.patch("ska_sdp_piper.app.cli.subprocess")
def test_should_run_benchmark_setup_from_cli(subprocess_mock, os_mock):

    os_mock.path.dirname.return_value = "SCRIPT_DIR"
    os_mock.path.exists.return_value = False
    benchmark(None, command=None, setup=True)

    os_mock.path.exists.assert_called_once_with("SCRIPT_DIR/dool")
    subprocess_mock.run.assert_called_once_with(
        ["SCRIPT_DIR/setup-dool.sh", "SCRIPT_DIR/dool"]
    )


@mock.patch("ska_sdp_piper.app.cli.os")
@mock.patch("ska_sdp_piper.app.cli.subprocess")
def test_should_not_setup_dool_if_already_present(subprocess_mock, os_mock):

    os_mock.path.dirname.return_value = "SCRIPT_DIR"
    os_mock.path.exists.return_value = True
    benchmark(None, command=None, setup=True)

    os_mock.path.exists.assert_called_once_with("SCRIPT_DIR/dool")
    subprocess_mock.run.assert_not_called()


@mock.patch("ska_sdp_piper.app.cli.os")
@mock.patch("ska_sdp_piper.app.cli.subprocess")
def test_should_show_ctx_help_if_no_argument_is_provided(
    subprocess_mock, os_mock
):

    os_mock.path.dirname.return_value = "SCRIPT_DIR"
    os_mock.path.exists.return_value = True
    ctx = Mock(name="ctx")
    benchmark(ctx, command=None, setup=False)

    ctx.command.get_help.assert_called_once_with(ctx)


@mock.patch("ska_sdp_piper.app.cli.os")
@mock.patch("ska_sdp_piper.app.cli.subprocess")
def test_should_run_benchmark_on_local_for_the_command(
    subprocess_mock, os_mock
):

    os_mock.path.dirname.return_value = "SCRIPT_DIR"
    os_mock.path.exists.return_value = True
    os_mock.getenv.return_value = "OS_ENV"
    ctx = Mock(name="ctx")
    benchmark(
        ctx,
        command="test command additional params --option value",
        setup=False,
        output_path="output",
        capture_interval=1,
    )

    subprocess_mock.run.assert_called_once_with(
        [
            "SCRIPT_DIR/run-dool.sh",
            "output",
            "test",
            "test command additional params --option value",
        ],
        env={
            "DOOL_BIN": "SCRIPT_DIR/dool/dool",
            "PATH": "OS_ENV",
            "DELAY_IN_SECONDS": 1,
        },
    )


@mock.patch("ska_sdp_piper.app.cli.os")
@mock.patch("ska_sdp_piper.app.cli.subprocess")
def test_should_raise_exception_if_dool_is_not_present(
    subprocess_mock, os_mock
):

    os_mock.path.dirname.return_value = "SCRIPT_DIR"
    os_mock.path.exists.return_value = False
    os_mock.getenv.return_value = "OS_ENV"
    ctx = Mock(name="ctx")
    with pytest.raises(RuntimeError):
        benchmark(
            ctx,
            command="test command additional params --option value",
            setup=False,
            output_path="output",
            capture_interval=1,
        )
