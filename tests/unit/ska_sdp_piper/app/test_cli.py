from mock import mock

from ska_sdp_piper.app.cli import install, uninstall


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
