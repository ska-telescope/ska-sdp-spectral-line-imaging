import pytest
from mock import MagicMock, Mock, mock

from ska_sdp_pipelines.app.executable_pipeline import (
    MAIN_ENTRY_POINT,
    ExecutablePipeline,
)


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.sys")
@mock.patch("builtins.open")
def test_should_prepare_executable(open_mock, sys_mock):
    sys_mock.executable = "/path/to/bin/python"
    file_obj = Mock("file_obj")
    file_obj.readlines = Mock("readlines", return_value="script content")
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = file_obj
    open_mock.return_value = enter_mock

    executable_pipeline = ExecutablePipeline("/path/to/script")

    executable_pipeline.prepare_executable()
    assert (
        executable_pipeline.executable_content
        == "#! /path/to/bin/python \n\n" + "script content" + MAIN_ENTRY_POINT
    )


@mock.patch(
    "ska_sdp_pipelines.app.executable_pipeline.os.path.exists",
    return_value=True,
)
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.importlib.util")
def test_should_validate_executable(util_mock, exists_mock):
    mock_spec = Mock("SPEC")
    mock_spec.loader = Mock("loader")
    mock_spec.loader.exec_module = Mock("exec_module")
    util_mock.spec_from_file_location.return_value = mock_spec
    util_mock.module_from_spec.return_value = "SPEC"
    executable_pipeline = ExecutablePipeline("/path/to/script")

    executable_pipeline.validate_pipeline()
    util_mock.spec_from_file_location.assert_called_once_with(
        "installable_pipeline", "/path/to/script"
    )
    util_mock.module_from_spec.assert_called_once_with(mock_spec)
    mock_spec.loader.exec_module.assert_called_once_with("SPEC")
    assert executable_pipeline.installable_pipeline == "SPEC"


@mock.patch(
    "ska_sdp_pipelines.app.executable_pipeline.os.path.exists",
    return_value=False,
)
def test_should_raise_exception_if_script_doesnot_exists(os_mock):
    executable_pipeline = ExecutablePipeline("/path/to/script")
    with pytest.raises(FileNotFoundError):
        executable_pipeline.validate_pipeline()


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.os")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Pipeline")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.sys")
@mock.patch("builtins.open")
def test_should_install_executable(
    open_mock, sys_mock, pipeline_mock, os_mock
):
    sys_mock.executable = "/path/to/bin/python"
    file_obj = Mock("file_obj")
    file_obj.write = Mock("readlines")
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = file_obj
    open_mock.return_value = enter_mock

    pipeline_mock.get_instance.return_value = pipeline_mock
    pipeline_mock.name = "PIPELINE"

    executable_pipeline = ExecutablePipeline("/path/to/script")
    executable_pipeline.executable_content = "script content"

    executable_pipeline.install()

    open_mock.assert_called_once_with("/path/to/bin/PIPELINE", "w")
    file_obj.write.assert_called_once_with("script content")
    os_mock.chmod.assert_called_once_with("/path/to/bin/PIPELINE", 0o750)


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.os")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Pipeline")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.sys")
def test_should_uninstall_executable(sys_mock, pipeline_mock, os_mock):
    sys_mock.executable = "/path/to/bin/python"

    pipeline_mock.get_instance.return_value = pipeline_mock
    pipeline_mock.name = "PIPELINE"

    executable_pipeline = ExecutablePipeline("/path/to/script")

    executable_pipeline.uninstall()

    os_mock.remove.assert_called_once_with("/path/to/bin/PIPELINE")
