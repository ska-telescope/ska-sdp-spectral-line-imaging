import pytest
from mock import MagicMock, Mock, mock

from ska_sdp_pipelines.app.executable_pipeline import (
    MAIN_ENTRY_POINT,
    ExecutablePipeline,
    PipelineNotFoundException,
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

    executable_pipeline = ExecutablePipeline(
        "pipeline_name", "/path/to/script"
    )

    executable_pipeline.prepare_executable()
    assert (
        executable_pipeline.executable_content
        == "#! /path/to/bin/python \n\n"
        + "script content"
        + MAIN_ENTRY_POINT.format(pipeline_name="pipeline_name")
    )


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Pipeline")
@mock.patch(
    "ska_sdp_pipelines.app.executable_pipeline.os.path.exists",
    return_value=True,
)
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.importlib.util")
def test_should_validate_executable(util_mock, exists_mock, pipeline_mock):
    mock_spec = Mock("SPEC")
    mock_spec.loader = Mock("loader")
    mock_spec.loader.exec_module = Mock("exec_module")
    util_mock.spec_from_file_location.return_value = mock_spec
    util_mock.module_from_spec.return_value = "SPEC"
    executable_pipeline = ExecutablePipeline(
        "pipeline_name", "/path/to/script"
    )

    executable_pipeline.validate_pipeline()
    util_mock.spec_from_file_location.assert_called_once_with(
        "installable_pipeline", "/path/to/script"
    )
    util_mock.module_from_spec.assert_called_once_with(mock_spec)
    mock_spec.loader.exec_module.assert_called_once_with("SPEC")
    pipeline_mock.assert_called_once_with(
        "pipeline_name", _existing_instance_=True
    )
    assert executable_pipeline.installable_pipeline == "SPEC"


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Pipeline")
@mock.patch(
    "ska_sdp_pipelines.app.executable_pipeline.os.path.exists",
    return_value=True,
)
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.importlib.util")
def test_should_raise_exception_if_pipeline_does_not_exists(
    util_mock, exists_mock, pipeline_mock
):
    mock_spec = Mock("SPEC")
    mock_spec.loader = Mock("loader")
    mock_spec.loader.exec_module = Mock("exec_module")
    util_mock.spec_from_file_location.return_value = mock_spec
    util_mock.module_from_spec.return_value = "SPEC"

    pipeline_mock.return_value = None
    executable_pipeline = ExecutablePipeline(
        "pipeline_name", "/path/to/script"
    )

    with pytest.raises(PipelineNotFoundException):
        executable_pipeline.validate_pipeline()


@mock.patch(
    "ska_sdp_pipelines.app.executable_pipeline.os.path.exists",
    return_value=False,
)
def test_should_raise_exception_if_script_doesnot_exists(os_mock):
    executable_pipeline = ExecutablePipeline(
        "pipeline_name", "/path/to/script"
    )
    with pytest.raises(FileNotFoundError):
        executable_pipeline.validate_pipeline()


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Path")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.os")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Pipeline")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.sys")
@mock.patch("builtins.open")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.write_yml")
def test_should_install_executable(
    write_yml_mock, open_mock, sys_mock, pipeline_mock, os_mock, path_mock
):
    sys_mock.executable = "/path/to/bin/python"
    file_obj = Mock("file_obj")
    file_obj.write = Mock("readlines")
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = file_obj
    open_mock.return_value = enter_mock

    path_mock.return_value = path_mock
    path_mock.parent.absolute.return_value = "/path/to/bin"
    os_mock.path.exists.return_value = True

    pipeline_mock.return_value = pipeline_mock
    pipeline_mock.name = "PIPELINE"

    executable_pipeline = ExecutablePipeline("PIPELINE", "/path/to/script")
    executable_pipeline.executable_content = "script content"

    executable_pipeline.install()

    pipeline_mock.assert_has_calls(
        [
            mock.call("PIPELINE", _existing_instance_=True),
            mock.call("PIPELINE", _existing_instance_=True),
        ]
    )

    open_mock.assert_has_calls([mock.call("/path/to/bin/PIPELINE", "w")])
    file_obj.write.assert_called_once_with("script content")
    os_mock.chmod.assert_called_once_with("/path/to/bin/PIPELINE", 0o750)


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Path")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.write_yml")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.os")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Pipeline")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.sys")
@mock.patch("builtins.open")
def test_should_write_configutartion_during_install(
    open_mock, sys_mock, pipeline_mock, os_mock, write_yml_mock, path_mock
):
    sys_mock.executable = "/global/path/to/bin/python"
    file_obj = Mock("file_obj")
    file_obj.write = Mock("readlines")
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = file_obj
    open_mock.return_value = enter_mock
    os_mock.path.exists.return_value = True

    path_mock.return_value = path_mock
    path_mock.parent.absolute.return_value = "/path/to/config"

    pipeline_mock.return_value = pipeline_mock
    pipeline_mock.name = "PIPELINE"
    pipeline_mock.config = "PIPELINE CONFIG"

    executable_pipeline = ExecutablePipeline("PIPELINE", "/path/to/script")
    executable_pipeline.executable_content = "script content"

    executable_pipeline.install()
    pipeline_mock.assert_has_calls(
        [
            mock.call("PIPELINE", _existing_instance_=True),
            mock.call("PIPELINE", _existing_instance_=True),
        ]
    )

    path_mock.assert_has_calls([mock.call("/path/to/script")])
    write_yml_mock.assert_called_once_with(
        "/path/to/config/PIPELINE.yaml", "PIPELINE CONFIG"
    )


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Path")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.write_yml")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.os")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Pipeline")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.sys")
@mock.patch("builtins.open")
def test_should_write_configutartion_during_install_to_provided_path(
    open_mock, sys_mock, pipeline_mock, os_mock, write_yml_mock, path_mock
):
    sys_mock.executable = "/global/path/to/bin/python"
    file_obj = Mock("file_obj")
    file_obj.write = Mock("readlines")
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = file_obj
    open_mock.return_value = enter_mock
    os_mock.path.exists.return_value = True

    path_mock.return_value = path_mock
    path_mock.parent.absolute.return_value = "/path/to/config"

    pipeline_mock.return_value = pipeline_mock
    pipeline_mock.name = "PIPELINE"
    pipeline_mock.config = "PIPELINE CONFIG"

    executable_pipeline = ExecutablePipeline("PIPELINE", "/path/to/script")
    executable_pipeline.executable_content = "script content"

    executable_pipeline.install("/new/path/to/config")

    pipeline_mock.assert_has_calls(
        [
            mock.call("PIPELINE", _existing_instance_=True),
            mock.call("PIPELINE", _existing_instance_=True),
        ]
    )
    write_yml_mock.assert_called_once_with(
        "/new/path/to/config/PIPELINE.yaml", "PIPELINE CONFIG"
    )


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Path")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.write_yml")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.os")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Pipeline")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.sys")
@mock.patch("builtins.open")
def test_should_raise_exception_during_install_if_config_path_does_not_exist(
    open_mock, sys_mock, pipeline_mock, os_mock, write_yml_mock, path_mock
):
    sys_mock.executable = "/global/path/to/bin/python"
    file_obj = Mock("file_obj")
    file_obj.write = Mock("readlines")
    enter_mock = MagicMock()
    enter_mock.__enter__.return_value = file_obj
    open_mock.return_value = enter_mock
    os_mock.path.exists.return_value = False

    executable_pipeline = ExecutablePipeline(
        "pipeline_name", "/path/to/script"
    )
    with pytest.raises(FileNotFoundError):
        executable_pipeline.install("/new/path/to/config")


@mock.patch("ska_sdp_pipelines.app.executable_pipeline.os")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.Pipeline")
@mock.patch("ska_sdp_pipelines.app.executable_pipeline.sys")
def test_should_uninstall_executable(sys_mock, pipeline_mock, os_mock):
    sys_mock.executable = "/path/to/bin/python"

    pipeline_mock.return_value = pipeline_mock
    pipeline_mock.name = "PIPELINE"

    executable_pipeline = ExecutablePipeline("PIPELINE", "/path/to/script")

    executable_pipeline.uninstall()

    pipeline_mock.assert_called_once_with("PIPELINE", _existing_instance_=True)
    os_mock.remove.assert_called_once_with("/path/to/bin/PIPELINE")
