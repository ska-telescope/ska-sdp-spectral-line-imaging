# pylint: disable=no-member
import mock
import numpy as np
import xarray as xr
from mock import MagicMock, Mock, call

from ska_sdp_spectral_line_imaging.stages.model import (
    cont_sub,
    read_model,
    vis_stokes_conversion,
)
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.convert_polarization")
def test_should_convert_polarization_of_observation_data(
    convert_polarization_mock,
):
    upout = UpstreamOutput()
    upout["ps"] = "observation_data"
    convert_polarization_mock.return_value = "converted_obs_data"

    output = vis_stokes_conversion.stage_definition(upout, ["I", "Q", "V"])

    convert_polarization_mock.assert_called_once_with(
        "observation_data", ["I", "Q", "V"]
    )
    assert output.ps == "converted_obs_data"


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.export_to_zarr")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.report_peak_visibility"
)
def test_should_perform_continuum_subtraction_and_export_residual(
    report_peak_mock, subtract_visibility_mock, export_to_zarr_mock
):

    observation = Mock(name="observation")
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    observation.assign.return_value = "model"
    cont_sub_observation = Mock(name="cont_sub_observation")
    cont_sub_observation.VISIBILITY.assign_attrs.return_value = (
        "sub_vis_with_attrs"
    )
    cont_sub_observation.frequency.units = ["Hz"]
    cont_sub_observation.assign.return_value = cont_sub_observation
    subtract_visibility_mock.return_value = cont_sub_observation

    output = cont_sub.stage_definition(
        upstream_output, True, "residual_visibility", False, "output_path"
    )

    observation.assign.assert_called_once_with(
        {"VISIBILITY": observation.VISIBILITY_MODEL}
    )
    subtract_visibility_mock.assert_called_once_with(observation, "model")
    export_to_zarr_mock.assert_called_once_with(
        cont_sub_observation.VISIBILITY,
        "output_path/residual_visibility",
        clear_attrs=True,
    )
    cont_sub_observation.assign.assert_called_once_with(
        {"VISIBILITY": "sub_vis_with_attrs"}
    )
    report_peak_mock.assert_called_once_with(
        cont_sub_observation.VISIBILITY, "Hz"
    )
    assert output.ps == cont_sub_observation.copy()
    assert output.compute_tasks == [
        export_to_zarr_mock.return_value,
        report_peak_mock.return_value,
    ]


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.logger")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.delayed_log")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.report_peak_visibility"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.fit_polynomial_on_visibility"
)
def test_should_report_extent_of_continuum_subtraction(
    fit_poly_on_vis_mock,
    numpy_mock,
    report_peak_mock,
    subtract_visibility_mock,
    delayed_log_mock,
    logger_mock,
):
    fit_poly_on_vis_mock.side_effect = [
        ["fit_real0", "fit_real1"],
        ["fit_imag0", "fit_imag1"],
    ]

    observation = MagicMock(name="observation")
    observation.polarization.values = ["RR", "LL"]
    flagged_vis = MagicMock(name="flagged_vis")
    observation.VISIBILITY.where.return_value = flagged_vis
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    subtract_visibility_mock.return_value = observation
    observation.assign.return_value = observation

    cont_sub.stage_definition(
        upstream_output, False, "ps_out", True, "output_path"
    )

    # pylint: disable=no-member
    observation.VISIBILITY.where.assert_called_once_with(
        numpy_mock.logical_not(observation.FLAG)
    )

    fit_poly_on_vis_mock.assert_has_calls(
        [
            call(flagged_vis.real),
            call(flagged_vis.imag),
        ]
    )

    delayed_log_mock.assert_has_calls(
        [
            call(
                logger_mock.info,
                "Slope of fit on real part {fit}",
                fit="fit_real1",
            ),
            call(
                logger_mock.info,
                "Slope of fit on imag part {fit}",
                fit="fit_imag1",
            ),
        ]
    )


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.logger")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.report_peak_visibility"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.fit_polynomial_on_visibility"
)
def test_should_not_report_extent_of_continuum_subtraction_for_invalid_pols(
    fit_poly_on_vis_mock,
    numpy_mock,
    subtract_visibility_mock,
    peak_log_mock,
    logger_mock,
):
    fit_poly_on_vis_mock.side_effect = ["fit_real", "fit_imag"]

    observation = MagicMock(name="observation")
    observation.polarization.values = ["RR", "LR"]
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    subtract_visibility_mock.return_value = observation

    observation.frequency = Mock(name="frequency")
    observation.frequency.units = ["Hz"]
    observation.assign.return_value = observation

    cont_sub.stage_definition(
        upstream_output, False, "ps_out", True, "output_path"
    )

    logger_mock.warning.assert_called_once_with(
        "Cannot report extent of continuum subtraction."
    )
    fit_poly_on_vis_mock.assert_not_called()
    peak_log_mock.assert_called_once_with(observation.VISIBILITY, "Hz")


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.get_dataarray_from_fits"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.apply_power_law_scaling"
)
def test_read_model_continuum_fits_with_pol_wcs_axis_with_power_law(
    apply_power_law_scaling_mock,
    get_data_from_fits_mock,
):
    ps = MagicMock(name="ps")
    pols = ["RR", "LL"]
    ps.polarization.values = pols
    ps.frequency.data = "frequency_data"

    upout = UpstreamOutput()
    upout["ps"] = ps

    fits_image1 = xr.DataArray(
        data=np.arange(4).reshape((1, 1, 2, 2)).astype(np.float32),
        dims=["polarization", "frequency", "y", "x"],
        coords={"polarization": [pols[0]]},
        name="cont_fits_image",
    )
    fits_image2 = xr.DataArray(
        data=np.arange(4).reshape((1, 1, 2, 2)).astype(np.float32),
        dims=["polarization", "frequency", "y", "x"],
        coords={"polarization": [pols[1]]},
        name="cont_fits_image",
    )
    get_data_from_fits_mock.side_effect = [fits_image1, fits_image2]

    expected_input_to_power_law = xr.DataArray(
        np.array([[[[0, 1], [2, 3]]], [[[0, 1], [2, 3]]]], dtype=np.float32),
        dims=["polarization", "frequency", "y", "x"],
        coords={"polarization": pols},
    )
    scaled_cube = MagicMock(name="scaled_cube")
    scaled_cube.chunk.return_value = "rechunked_scaled_cube"
    apply_power_law_scaling_mock.return_value = scaled_cube

    output = read_model.stage_definition(
        upout,
        "test-%s-image.fits",
        do_power_law_scaling=True,
        spectral_index=0.1,
    )

    get_data_from_fits_mock.assert_has_calls(
        [mock.call("test-RR-image.fits"), mock.call("test-LL-image.fits")]
    )

    actual_input_to_power_law = apply_power_law_scaling_mock.call_args.args[0]
    xr.testing.assert_allclose(
        actual_input_to_power_law,
        expected_input_to_power_law,
    )
    assert (
        actual_input_to_power_law.chunks == expected_input_to_power_law.chunks
    )
    assert apply_power_law_scaling_mock.call_args.args[1] == "frequency_data"
    assert (
        apply_power_law_scaling_mock.call_args.kwargs["spectral_index"] == 0.1
    )
    scaled_cube.chunk.assert_called_once_with({"polarization": -1})
    assert output["model_image"] == "rechunked_scaled_cube"


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.get_dataarray_from_fits"
)
def test_should_read_model_from_continuum_fits_without_pol_axis(
    get_data_from_fits_mock,
):
    ps = MagicMock(name="ps")
    pols = ["I", "V"]
    ps.polarization.values = pols

    upout = UpstreamOutput()
    upout["ps"] = ps

    fits_image = xr.DataArray(
        data=np.arange(4).reshape((1, 2, 2)).astype(np.float32),
        dims=["frequency", "y", "x"],
        coords={},
        name="cont_fits_image",
    )

    get_data_from_fits_mock.return_value = fits_image

    expected_dataarray = xr.DataArray(
        np.array([[[0, 1], [2, 3]], [[0, 1], [2, 3]]], dtype=np.float32),
        dims=["polarization", "y", "x"],
        coords={"polarization": pols},
    ).chunk()

    output = read_model.stage_definition(
        upout,
        "test-%s-image.fits",
        do_power_law_scaling=False,
        spectral_index=0.1,
    )

    get_data_from_fits_mock.assert_has_calls(
        [mock.call("test-I-image.fits"), mock.call("test-V-image.fits")]
    )
    xr.testing.assert_allclose(expected_dataarray, output["model_image"])

    assert expected_dataarray.chunks == output["model_image"].chunks
