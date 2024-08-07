from pathlib import Path

from mock import Mock, mock

from ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser import (
    SpectralLineDiagnoser,
)


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".select_field"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".read_dataset"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
def test_should_initialise_diagnoser(
    read_yml_mock, xarray_open_mock, read_dataset_mock, select_field
):
    output_path = Mock(name="output", spec=Path)

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"
    residual = Mock(name="residual")
    residual.VISIBILITY = "RES_VISIBILITY"

    input_ps = Mock(name="input_ps")
    input_ps.VISIBILITY = "INPUT_VISIBILITY"

    select_field.stage_definition.return_value = {
        "ps": "SELECTED_INPUT_DATASET"
    }
    read_dataset_mock.return_value = "PROCESSING_SET"

    xarray_open_mock.side_effect = [
        {"VISIBILITY_MODEL": "model_data"},
        "residual_data",
    ]
    config = {
        "pipeline": {"export_model": True, "export_residual": True},
        "parameters": {
            "export_residual": {"psout_name": "ps_out_residual"},
            "export_model": {"psout_name": "ps_out_model"},
            "select_vis": {"arguments": "arguments"},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    SpectralLineDiagnoser(input_path, output_path)

    input_path.glob.assert_has_calls(
        [mock.call("*.cli.yml"), mock.call("*.config.yml")]
    )

    read_yml_mock.assert_has_calls(
        [mock.call("cli.yml"), mock.call("conf.yml")]
    )

    read_dataset_mock.assert_called_once_with("INPUT_DATA")
    select_field.stage_definition.assert_called_once_with(
        None, arguments="arguments", _input_data_="PROCESSING_SET"
    )

    xarray_open_mock.assert_has_calls(
        [mock.call("input/ps_out_model"), mock.call("input/ps_out_residual")]
    )


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".select_field"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".read_dataset"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
def test_should_initialise_diagnoser_without_model_residual(
    read_yml_mock, xarray_open_mock, read_dataset_mock, select_field
):
    output_path = Mock(name="output", spec=Path)

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"
    residual = Mock(name="residual")
    residual.VISIBILITY = "RES_VISIBILITY"

    input_ps = Mock(name="input_ps")
    input_ps.VISIBILITY = "INPUT_VISIBILITY"

    select_field.stage_definition.return_value = {
        "ps": "SELECTED_INPUT_DATASET"
    }
    read_dataset_mock.return_value = "PROCESSING_SET"

    config = {
        "pipeline": {"export_model": False, "export_residual": False},
        "parameters": {
            "export_residual": {"psout_name": "ps_out_residual"},
            "export_model": {"psout_name": "ps_out_model"},
            "select_vis": {"arguments": "arguments"},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    SpectralLineDiagnoser(input_path, output_path)

    assert xarray_open_mock.call_count == 0
    input_path.glob.assert_has_calls(
        [mock.call("*.cli.yml"), mock.call("*.config.yml")]
    )

    read_yml_mock.assert_has_calls(
        [mock.call("cli.yml"), mock.call("conf.yml")]
    )

    read_dataset_mock.assert_called_once_with("INPUT_DATA")
    select_field.stage_definition.assert_called_once_with(
        None, arguments="arguments", _input_data_="PROCESSING_SET"
    )


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".np.vectorize"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".amp_vs_channel_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".amp_vs_uv_distance_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".select_field"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".read_dataset"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
def test_should_not_plot_residual_and_model_if_not_exported(
    read_yml_mock,
    read_dataset_mock,
    select_field,
    amp_vs_uv_distance_plot_mock,
    amp_vs_channel_plot_mock,
    vectorize_mock,
    zarr_mock,
):

    vectorize_mock.return_value = Mock(name="vectorized_fn")
    vectorize_mock.return_value.return_value = "uv-distance"

    output_path = Mock(name="output", spec=Path)
    output_path.__truediv__ = lambda x, y: f"output/{y}"

    visibility = Mock(name="visibility")
    visibility.mean.return_value = "visibility"

    uvw = Mock(name="uvw")
    uvw.mean.return_value = "uvw"

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"
    residual = Mock(name="residual")
    residual.VISIBILITY = "RES_VISIBILITY"

    input_ps = Mock(name="input_ps")
    input_ps.UVW = uvw
    input_ps.VISIBILITY = visibility

    select_field.stage_definition.return_value = {"ps": input_ps}
    read_dataset_mock.return_value = input_ps

    config = {
        "pipeline": {"export_model": False, "export_residual": False},
        "parameters": {
            "export_residual": {"psout_name": "ps_out_residual"},
            "export_model": {"psout_name": "ps_out_model"},
            "select_vis": {"arguments": "arguments"},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    diagnoser = SpectralLineDiagnoser(input_path, output_path)
    diagnoser.diagnose()

    assert amp_vs_channel_plot_mock.call_count == 2
    assert amp_vs_uv_distance_plot_mock.call_count == 1
