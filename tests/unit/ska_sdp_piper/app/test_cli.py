import pytest
from mock import Mock, mock

from ska_sdp_piper.app.benchmark import DOOL_BIN
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
@mock.patch("ska_sdp_piper.app.cli.setup_dool")
def test_should_run_benchmark_setup_from_cli(setup_dool_mock, os_mock):
    os_mock.path.exists.return_value = False

    benchmark(None, command=None, setup=True)

    os_mock.path.exists.assert_called_once_with(DOOL_BIN)
    setup_dool_mock.assert_called_once_with()


@mock.patch("ska_sdp_piper.app.cli.os")
@mock.patch("ska_sdp_piper.app.cli.setup_dool")
def test_should_not_setup_dool_if_already_present(setup_dool_mock, os_mock):
    os_mock.path.exists.return_value = True

    benchmark(None, command=None, setup=True)

    os_mock.path.exists.assert_called_once_with(DOOL_BIN)
    setup_dool_mock.assert_not_called()


def test_should_show_ctx_help_if_no_argument_is_provided():
    ctx = Mock(name="ctx")
    benchmark(ctx, command=None, setup=False)
    ctx.command.get_help.assert_called_once_with(ctx)


@mock.patch("ska_sdp_piper.app.cli.os")
@mock.patch("ska_sdp_piper.app.cli.run_dool")
@mock.patch("ska_sdp_piper.app.cli.setup_dool")
def test_should_run_benchmark_on_local_for_the_command(
    setup_dool_mock, run_dool_mock, os_mock
):
    ctx = Mock(name="ctx")

    benchmark(
        ctx,
        command="test command --option value",
        setup=False,
        output_path="./benchmark",
        capture_interval=5,
        output_file_prefix=None,
    )

    setup_dool_mock.assert_not_called()
    run_dool_mock.assert_called_once_with(
        output_dir="./benchmark",
        file_prefix="test",
        command=["test", "command", "--option", "value"],
        capture_interval=5,
    )


@mock.patch("ska_sdp_piper.app.cli.os")
@mock.patch("ska_sdp_piper.app.cli.run_dool")
@mock.patch("ska_sdp_piper.app.cli.setup_dool")
def test_should_run_benchmark_on_local_with_file_prefix(
    setup_dool_mock, run_dool_mock, os_mock
):
    ctx = Mock(name="ctx")
    os_mock.path.exists.side_effect = [False, True]

    benchmark(
        ctx,
        command="test command --option value",
        setup=True,
        output_path="output",
        capture_interval=100,
        output_file_prefix="run_1",
    )

    os_mock.path.exists.assert_has_calls(
        [
            mock.call(DOOL_BIN),
            mock.call(DOOL_BIN),
        ]
    )
    setup_dool_mock.assert_called_once_with()
    run_dool_mock.assert_called_once_with(
        output_dir="output",
        file_prefix="run_1_test",
        command=["test", "command", "--option", "value"],
        capture_interval=100,
    )


@mock.patch("ska_sdp_piper.app.cli.os")
@mock.patch("ska_sdp_piper.app.cli.run_dool")
@mock.patch("ska_sdp_piper.app.cli.setup_dool")
def test_should_raise_exception_if_dool_is_not_present(
    setup_dool_mock, run_dool_mock, os_mock
):
    os_mock.path.exists.return_value = False

    ctx = Mock(name="ctx")
    with pytest.raises(
        RuntimeError, match="Dool not found, please run with `--setup` option"
    ):
        benchmark(
            ctx,
            command="test command --option value",
            setup=False,
        )

    setup_dool_mock.assert_not_called()
    run_dool_mock.assert_not_called()
