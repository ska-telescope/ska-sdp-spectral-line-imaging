from pathlib import Path

import numpy as np
import pytest
import xarray
from mock import Mock, mock

from ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser import (
    SpectralLineDiagnoser,
    get_uv_dist,
)


@pytest.fixture(scope="function")
def default_scheduler():
    with mock.patch(
        "ska_sdp_spectral_line_imaging.scheduler.DefaultScheduler"
    ) as default_scheduler_mock:
        yield default_scheduler_mock


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".load_data"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
def test_should_initialise_diagnoser(
    read_yml_mock,
    xarray_open_mock,
    load_data_mock,
    default_scheduler,
):
    output_path = Mock(name="output", spec=Path)
    channel = 1
    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"
    residual = Mock(name="residual")
    residual.VISIBILITY = "RES_VISIBILITY"

    input_ps = Mock(name="input_ps")
    input_ps.VISIBILITY = "INPUT_VISIBILITY"

    load_data_mock.stage_definition.return_value = {
        "ps": "SELECTED_INPUT_DATASET"
    }

    xarray_open_mock.side_effect = [
        {"VISIBILITY_MODEL": "model_data"},
        "residual_data",
    ]
    config = {
        "pipeline": {"continuum_subtraction": True, "predict_stage": True},
        "parameters": {
            "continuum_subtraction": {
                "psout_name": "ps_out_residual",
                "export_residual": True,
            },
            "predict_stage": {
                "psout_name": "ps_out_model",
                "export_model": True,
            },
            "load_data": {"arguments": "arguments"},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    SpectralLineDiagnoser(
        input_path, output_path, channel, scheduler=default_scheduler
    )

    input_path.glob.assert_has_calls(
        [mock.call("*.cli.yml"), mock.call("*.config.yml")]
    )

    read_yml_mock.assert_has_calls(
        [mock.call("cli.yml"), mock.call("conf.yml")]
    )

    load_data_mock.stage_definition.assert_called_once_with(
        default_scheduler._stage_outputs,
        arguments="arguments",
        _cli_args_={"input": "INPUT_DATA"},
    )

    xarray_open_mock.assert_has_calls(
        [
            mock.call("input/ps_out_model.zarr"),
            mock.call("input/ps_out_residual.zarr"),
        ]
    )


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".load_data"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
def test_should_initialise_diagnoser_without_model_residual(
    read_yml_mock,
    xarray_open_mock,
    load_data_mock,
    default_scheduler,
):
    output_path = Mock(name="output", spec=Path)
    channel = 1
    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"
    residual = Mock(name="residual")
    residual.VISIBILITY = "RES_VISIBILITY"

    input_ps = Mock(name="input_ps")
    input_ps.VISIBILITY = "INPUT_VISIBILITY"

    load_data_mock.stage_definition.return_value = {
        "ps": "SELECTED_INPUT_DATASET"
    }

    config = {
        "pipeline": {"continuum_subtraction": False, "predict_stage": False},
        "parameters": {
            "continuum_subtraction": {
                "psout_name": "ps_out_residual",
                "export_residual": True,
            },
            "predict_stage": {
                "psout_name": "ps_out_model",
                "export_model": True,
            },
            "load_data": {"arguments": "arguments"},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    SpectralLineDiagnoser(
        input_path, output_path, channel, scheduler=default_scheduler
    )

    assert xarray_open_mock.call_count == 0
    input_path.glob.assert_has_calls(
        [mock.call("*.cli.yml"), mock.call("*.config.yml")]
    )

    read_yml_mock.assert_has_calls(
        [mock.call("cli.yml"), mock.call("conf.yml")]
    )

    load_data_mock.stage_definition.assert_called_once_with(
        default_scheduler._stage_outputs,
        arguments="arguments",
        _cli_args_={"input": "INPUT_DATA"},
    )


@pytest.fixture(scope="function")
def test_data():
    with mock.patch(
        "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
        ".xr.where"
    ) as where_mock:
        visibility = Mock(name="visibility")
        visibility.mean.return_value = visibility
        visibility.sel.return_value = visibility
        visibility.isel.return_value = visibility
        visibility.values = [1, 2, 3, 4]

        where_mock.return_value = visibility

        uvw = Mock(name="uvw")
        uvw.mean.return_value = "uvw"

        input_ps = Mock(name="input_ps")
        input_ps.UVW = uvw
        input_ps.VISIBILITY = visibility
        input_ps.polarization = xarray.DataArray(["XX", "XY", "YX", "YY"])

        residual = Mock(name="residual")
        residual.VISIBILITY = visibility
        residual.polarization = xarray.DataArray(["I", "Q", "U", "V"])
        residual.frequency = xarray.DataArray([1, 2, 3])

        model = Mock(name="residual")
        model.VISIBILITY = visibility
        model.polarization = xarray.DataArray(["I", "Q", "U", "V"])
        model.frequency = xarray.DataArray([1, 2, 3])

        yield (input_ps, residual, model)


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".store_spectral_csv"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".get_uv_dist"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.np.abs"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".amp_vs_channel_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".create_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".load_data"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".convert_polarization"
)
def test_should_plot_residual_and_model(
    convert_polarization_mock,
    read_yml_mock,
    load_data_mock,
    create_plot_mock,
    amp_vs_channel_plot_mock,
    abs_mock,
    zarr_mock,
    uv_dist_mock,
    store_spectral_csv_mock,
    test_data,
    default_scheduler,
):
    uv_dist_mock.return_value = "uv-distance"

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"

    output_path = Mock(name="output", spec=Path)
    output_path.__truediv__ = lambda x, y: f"output/{y}"

    channel = 1

    input_ps, residual, model = test_data

    default_scheduler._stage_outputs.ps = input_ps

    zarr_mock.side_effect = [
        {"VISIBILITY_MODEL": model},
        residual,
    ]

    config = {
        "pipeline": {"continuum_subtraction": True, "predict_stage": True},
        "parameters": {
            "continuum_subtraction": {
                "psout_name": "ps_out_residual",
                "export_residual": True,
            },
            "predict_stage": {
                "psout_name": "ps_out_model",
                "export_model": True,
            },
            "load_data": {"arguments": "arguments"},
            "read_model": {"pols": ["I", "Q", "U", "V"]},
        },
    }
    cli = {"input": "INPUT_DATA"}

    updated_input_ps = input_ps
    updated_input_ps.polarization = model.polarization
    convert_polarization_mock.return_value = updated_input_ps

    read_yml_mock.side_effect = [cli, config]

    diagnoser = SpectralLineDiagnoser(
        input_path, output_path, channel, scheduler=default_scheduler
    )
    diagnoser.diagnose()

    assert amp_vs_channel_plot_mock.call_count == 4
    assert create_plot_mock.call_count == 3

    amp_vs_channel_plot_mock.assert_has_calls(
        [
            mock.call(
                residual.VISIBILITY.sel("I"),
                title="Amp Vs Channel on Input Visibilities",
                path="output/single-pol-I-amp-vs-channel-input-vis.png",
                label="I",
            ),
            mock.call(
                residual.VISIBILITY,
                title="Amp Vs Channel on Input Visibilities",
                path="output/all-pol-amp-vs-channel-input-vis.png",
                label=input_ps.polarization.values,
            ),
            mock.call(
                residual.VISIBILITY.sel("I"),
                title="Amp Vs Channel on Residual Visibilities",
                path="output/single-pol-I-amp-vs-channel-residual-vis.png",
                label="I",
            ),
            mock.call(
                residual.VISIBILITY,
                title="Amp Vs Channel on Residual Visibilities",
                path="output/all-pol-amp-vs-channel-residual-vis.png",
                label=residual.polarization.values,
            ),
        ]
    )

    create_plot_mock.assert_has_calls(
        [
            mock.call(
                np.abs("uv-distance"),
                np.abs(input_ps.VISIBILITY),
                xlabel="uv distance",
                ylabel="amp",
                title="Amp vs UV Distance before Continnum Subtraction",
                path="output/amp-vs-uv-distance-before-cont-sub.png",
                label=None,
            ),
            mock.call(
                np.abs("uv-distance"),
                np.abs(residual.VISIBILITY),
                xlabel="uv distance",
                ylabel="amp",
                title="Amp vs UV Distance after Continnum Subtraction",
                path="output/amp-vs-uv-distance-after-cont-sub.png",
                label=None,
            ),
            mock.call(
                np.abs("uv-distance"),
                np.abs(model.VISIBILITY),
                xlabel="uv distance",
                ylabel="amp",
                title="Amp vs UV Distance model",
                path="output/amp-vs-uv-distance-model.png",
                label=None,
            ),
        ]
    )


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".get_uv_dist"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.np.abs"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".amp_vs_channel_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".create_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".load_data"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".convert_polarization"
)
def test_should_not_plot_residual_and_model_if_not_exported(
    convert_polarization_mock,
    read_yml_mock,
    load_data_mock,
    create_plot_mock,
    amp_vs_channel_plot_mock,
    abs_mock,
    zarr_mock,
    uv_dist_mock,
    test_data,
    default_scheduler,
):
    uv_dist_mock.return_value = "uv-distance"

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"

    output_path = Mock(name="output", spec=Path)
    output_path.__truediv__ = lambda x, y: f"output/{y}"

    channel = 1

    input_ps, residual, _ = test_data

    load_data_mock.stage_definition.return_value = {"ps": input_ps}

    zarr_mock.return_value = residual

    config = {
        "pipeline": {"continuum_subtraction": False, "predict_stage": False},
        "parameters": {
            "continuum_subtraction": {
                "psout_name": "ps_out_residual",
                "export_residual": True,
            },
            "predict_stage": {
                "psout_name": "ps_out_model",
                "export_model": True,
            },
            "load_data": {"arguments": "arguments"},
            "read_model": {"pols": ["I", "Q", "U", "V"]},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    diagnoser = SpectralLineDiagnoser(
        input_path, output_path, channel, scheduler=default_scheduler
    )
    diagnoser.diagnose()

    assert amp_vs_channel_plot_mock.call_count == 2
    assert create_plot_mock.call_count == 1


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".store_spectral_csv"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".get_uv_dist"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.np.abs"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".amp_vs_channel_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".create_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".load_data"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".convert_polarization"
)
def test_should_export_residual_csv(
    convert_polarization_mock,
    read_yml_mock,
    load_data_mock,
    create_plot_mock,
    amp_vs_channel_plot_mock,
    abs_mock,
    xarray_open_mock,
    uv_dist_mock,
    store_spectral_csv_mock,
    test_data,
    default_scheduler,
):
    uv_dist_mock.return_value = "uv-distance"

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"

    output_path = Mock(name="output", spec=Path)
    output_path.__truediv__ = lambda x, y: f"output/{y}"

    channel = 1

    input_ps, residual, _ = test_data

    load_data_mock.stage_definition.return_value = {"ps": input_ps}

    xarray_open_mock.return_value = residual

    config = {
        "pipeline": {"continuum_subtraction": True, "predict_stage": True},
        "parameters": {
            "continuum_subtraction": {
                "psout_name": "ps_out_residual",
                "export_residual": True,
            },
            "predict_stage": {
                "psout_name": "ps_out_model",
                "export_model": False,
            },
            "load_data": {"arguments": "arguments"},
            "read_model": {"pols": ["I", "Q", "U", "V"]},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    abs_mock.return_value = [1, 2, 3, 4]

    diagnoser = SpectralLineDiagnoser(
        input_path, output_path, channel, scheduler=default_scheduler
    )
    diagnoser.diagnose()

    store_spectral_csv_mock.assert_called_once_with(
        residual.frequency.values, [1, 2, 3, 4], "output/residual.csv"
    )


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".get_uv_dist"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.np.abs"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".amp_vs_channel_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".create_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".load_data"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".read_yml"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".convert_polarization"
)
def test_should_convert_input_polarization_to_that_of_model(
    convert_polarization_mock,
    read_yml_mock,
    load_data_mock,
    create_plot_mock,
    amp_vs_channel_plot_mock,
    abs_mock,
    zarr_mock,
    uv_dist_mock,
    test_data,
    default_scheduler,
):
    uv_dist_mock.return_value = "uv-distance"

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"

    output_path = Mock(name="output", spec=Path)
    output_path.__truediv__ = lambda x, y: f"output/{y}"

    channel = 1

    input_ps, residual, model = test_data

    default_scheduler._stage_outputs.ps = input_ps

    zarr_mock.side_effect = [
        {"VISIBILITY_MODEL": model},
        residual,
    ]

    config = {
        "pipeline": {"continuum_subtraction": True, "predict_stage": True},
        "parameters": {
            "continuum_subtraction": {
                "psout_name": "ps_out_residual",
                "export_residual": True,
            },
            "predict_stage": {
                "psout_name": "ps_out_model",
                "export_model": True,
            },
            "load_data": {"arguments": "arguments"},
            "read_model": {"pols": ["I", "Q", "U", "V"]},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    diagnoser = SpectralLineDiagnoser(
        input_path, output_path, channel, scheduler=default_scheduler
    )
    diagnoser.diagnose()

    convert_polarization_mock.assert_called_once_with(
        input_ps, model.polarization.values
    )


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".get_uv_dist"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.np.abs"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".amp_vs_channel_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".create_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".load_data"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".convert_polarization"
)
def test_should_convert_input_pol_to_that_of_residual_if_model_is_not_exported(
    convert_polarization_mock,
    read_yml_mock,
    load_data_mock,
    create_plot_mock,
    amp_vs_channel_plot_mock,
    abs_mock,
    zarr_mock,
    uv_dist_mock,
    test_data,
    default_scheduler,
):
    uv_dist_mock.return_value = "uv-distance"

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"

    output_path = Mock(name="output", spec=Path)
    output_path.__truediv__ = lambda x, y: f"output/{y}"

    channel = 1

    input_ps, residual, _ = test_data

    default_scheduler._stage_outputs.ps = input_ps

    zarr_mock.return_value = residual

    config = {
        "pipeline": {"continuum_subtraction": True, "predict_stage": True},
        "parameters": {
            "continuum_subtraction": {
                "psout_name": "ps_out_residual",
                "export_residual": True,
            },
            "predict_stage": {
                "psout_name": "ps_out_model",
                "export_model": False,
            },
            "load_data": {"arguments": "arguments"},
            "read_model": {"pols": ["I", "Q", "U", "V"]},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    diagnoser = SpectralLineDiagnoser(
        input_path, output_path, channel, scheduler=default_scheduler
    )

    diagnoser.diagnose()

    convert_polarization_mock.assert_called_once_with(
        input_ps, residual.polarization.values
    )


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".get_uv_dist"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.np.abs"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".amp_vs_channel_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".create_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".load_data"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".convert_polarization"
)
def test_should_not_conver_input_when_model_and_residual_are_not_exported(
    convert_polarization_mock,
    read_yml_mock,
    load_data_mock,
    create_plot_mock,
    amp_vs_channel_plot_mock,
    abs_mock,
    zarr_mock,
    uv_dist_mock,
    test_data,
    default_scheduler,
):
    uv_dist_mock.return_value = "uv-distance"

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"

    output_path = Mock(name="output", spec=Path)
    output_path.__truediv__ = lambda x, y: f"output/{y}"

    channel = 1

    input_ps, _, _ = test_data

    default_scheduler._stage_outputs.ps = input_ps

    config = {
        "pipeline": {"continuum_subtraction": True, "predict_stage": True},
        "parameters": {
            "continuum_subtraction": {
                "psout_name": "ps_out_residual",
                "export_residual": False,
            },
            "predict_stage": {
                "psout_name": "ps_out_model",
                "export_model": False,
            },
            "load_data": {"arguments": "arguments"},
            "read_model": {"pols": ["I", "Q", "U", "V"]},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    diagnoser = SpectralLineDiagnoser(
        input_path, output_path, channel, scheduler=default_scheduler
    )

    diagnoser.diagnose()

    assert convert_polarization_mock.call_count == 0


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".get_uv_dist"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".xr.open_zarr"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.np.abs"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".amp_vs_channel_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".create_plot"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".load_data"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser.read_yml"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis.spectral_line_diagnoser"
    ".convert_polarization"
)
def test_should_not_conver_input_when_input_and_model_are_in_same_polarization(
    convert_polarization_mock,
    read_yml_mock,
    load_data_mock,
    create_plot_mock,
    amp_vs_channel_plot_mock,
    abs_mock,
    zarr_mock,
    uv_dist_mock,
    test_data,
    default_scheduler,
):
    uv_dist_mock.return_value = "uv-distance"

    input_path = Mock(name="input_path")
    input_path.glob.return_value = ["cli.yml", "conf.yml"].__iter__()
    input_path.__truediv__ = lambda x, y: f"input/{y}"

    output_path = Mock(name="output", spec=Path)
    output_path.__truediv__ = lambda x, y: f"output/{y}"

    channel = 1

    input_ps, residual, model = test_data
    input_ps.polarization = model.polarization

    default_scheduler._stage_outputs.ps = input_ps

    zarr_mock.side_effect = [
        {"VISIBILITY_MODEL": model},
        residual,
    ]

    config = {
        "pipeline": {"continuum_subtraction": True, "predict_stage": True},
        "parameters": {
            "continuum_subtraction": {
                "psout_name": "ps_out_residual",
                "export_residual": True,
            },
            "predict_stage": {
                "psout_name": "ps_out_model",
                "export_model": True,
            },
            "load_data": {"arguments": "arguments"},
            "read_model": {"pols": ["I", "Q", "U", "V"]},
        },
    }
    cli = {"input": "INPUT_DATA"}

    read_yml_mock.side_effect = [cli, config]

    diagnoser = SpectralLineDiagnoser(
        input_path, output_path, channel, scheduler=default_scheduler
    )

    diagnoser.diagnose()

    assert zarr_mock.call_count == 2
    assert convert_polarization_mock.call_count == 0


def test_should_get_uv_dist():
    mock_uvw = Mock(name="UVW", spec=xarray.DataArray)
    mock_uvw.transpose.return_value = [3, 4]
    uv_dist = get_uv_dist(mock_uvw)

    mock_uvw.transpose.assert_called_once_with(
        "uvw_label", "time", "baseline_id"
    )
    assert uv_dist == 5.0
