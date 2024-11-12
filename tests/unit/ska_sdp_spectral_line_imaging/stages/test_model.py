import mock
import numpy as np
import pytest
import xarray as xr
from mock import MagicMock, Mock, call

from ska_sdp_spectral_line_imaging.stages.model import (
    _fit_polynomial_on_visibility,
    apply_power_law_scaling,
    cont_sub,
    get_dask_array_from_fits,
)
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model."
    "np.polynomial.polynomial.polyfit"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.dask.delayed")
def test_should_fit_polynomial_on_visibility(dask_delayed_mock, polyfit_mock):
    data = np.arange(24).reshape(2, 2, 2, 3).astype(np.float32)
    data[0, 1, 0, 2] = np.nan
    data[1, 0, 1, 0] = np.nan
    array = xr.DataArray(
        data=data,
        dims=["time", "baseline_id", "polarization", "frequency"],
    )
    dask_delayed_mock.return_value = polyfit_mock
    polyfit_mock.return_value = [2, 1]

    result = _fit_polynomial_on_visibility(array)

    expected_xaxis = np.array([0, 1, 2])
    expected_data = xr.DataArray(
        data=[9.857142, 11.5, 13.142858], dims=["frequency"]
    )
    expected_weight = xr.DataArray(data=[1.0, 1.0, 1.0], dims=["frequency"])

    dask_delayed_mock.assert_called_once_with(polyfit_mock)

    actual_args = polyfit_mock.call_args.args
    actual_kwargs = polyfit_mock.call_args.kwargs
    np.allclose(actual_args[0], expected_xaxis)
    np.allclose(actual_args[1], expected_data)
    np.allclose(actual_kwargs["w"], expected_weight)
    np.allclose(actual_kwargs["deg"], 1)

    assert result == [2, 1]


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model."
    "np.polynomial.polynomial.polyfit"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.dask.delayed")
def test_should_filter_channel_when_all_values_are_nan(
    dask_delayed_mock, polyfit_mock
):
    data = np.arange(24).reshape(2, 2, 2, 3).astype(np.float32)
    data[:, :, :, 2] = np.nan
    array = xr.DataArray(
        data=data,
        dims=["time", "baseline_id", "polarization", "frequency"],
    )
    dask_delayed_mock.return_value = polyfit_mock

    _fit_polynomial_on_visibility(array)

    expected_xaxis = np.array([0, 1, 2])
    expected_data = xr.DataArray(data=[10.5, 11.5, 0.0], dims=["frequency"])
    expected_weight = xr.DataArray(data=[1.0, 1.0, 0.0], dims=["frequency"])

    dask_delayed_mock.assert_called_once_with(polyfit_mock)

    actual_args = polyfit_mock.call_args.args
    actual_kwargs = polyfit_mock.call_args.kwargs
    np.allclose(actual_args[0], expected_xaxis)
    np.allclose(actual_args[1], expected_data)
    np.allclose(actual_kwargs["w"], expected_weight)
    np.allclose(actual_kwargs["deg"], 1)


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
def test_should_perform_continuum_subtraction(
    np_mock, subtract_visibility_mock
):

    observation = Mock(name="observation")
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    observation.assign.return_value = "model"
    subtracted_vis = Mock(name="subtracted_vis")
    subtracted_vis.VISIBILITY.assign_attrs.return_value = "sub_vis_with_attrs"
    subtracted_vis.frequency = Mock(name="frequency")
    subtracted_vis.frequency.units = ["Hz"]
    subtracted_vis.assign.return_value = subtracted_vis
    subtract_visibility_mock.return_value = subtracted_vis

    output = cont_sub.stage_definition(
        upstream_output, False, "ps_out", False, "output_path"
    )

    observation.assign.assert_called_once_with(
        {"VISIBILITY": observation.VISIBILITY_MODEL}
    )
    subtract_visibility_mock.assert_called_once_with(observation, "model")
    subtracted_vis.assign.assert_called_once_with(
        {"VISIBILITY": "sub_vis_with_attrs"}
    )
    assert output.ps == subtracted_vis.copy()


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.logger")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.delayed_log")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
def test_should_report_peak_channel_value(
    numpy_mock, subtract_visibility_mock, delayed_log_mock, logger_mock
):

    observation = Mock(name="observation")
    observation.frequency = Mock(name="frequency")
    observation.frequency.units = ["Hz"]
    observation.assign.return_value = observation
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    subtract_visibility_mock.return_value = observation
    numpy_mock.abs = Mock(name="abs", return_value=numpy_mock)
    numpy_mock.max = Mock(name="max", return_value=numpy_mock)
    numpy_mock.idxmax = Mock(name="idxmax", return_value=numpy_mock)
    numpy_mock.argmax = Mock(name="idxmax", return_value=numpy_mock)

    cont_sub.stage_definition(
        upstream_output, False, "ps_out", False, "output_path"
    )

    numpy_mock.abs.assert_called_once_with(observation.VISIBILITY)
    numpy_mock.max.assert_has_calls(
        [mock.call(dim=["time", "baseline_id", "polarization"])]
    )
    numpy_mock.idxmax.assert_called_once()
    numpy_mock.argmax.assert_called_once()
    delayed_log_mock.assert_called_once_with(
        logger_mock.info,
        "Peak visibility Channel: {peak_channel}."
        " Frequency: {peak_frequency} {unit}."
        " Peak Visibility: {max_visibility}",
        peak_channel=numpy_mock,
        peak_frequency=numpy_mock,
        max_visibility=numpy_mock,
        unit="Hz",
    )


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.logger")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.delayed_log")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model._fit_polynomial_on_visibility"
)
def test_should_report_extent_of_continuum_subtraction(
    fit_poly_on_vis_mock,
    numpy_mock,
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
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.delayed_log")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model._fit_polynomial_on_visibility"
)
def test_should_not_report_extent_of_continuum_subtraction_for_invalid_pols(
    fit_poly_on_vis_mock,
    numpy_mock,
    subtract_visibility_mock,
    delayed_log_mock,
    logger_mock,
):
    fit_poly_on_vis_mock.side_effect = ["fit_real", "fit_imag"]

    observation = MagicMock(name="observation")
    observation.polarization.values = ["RR", "LR"]
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    subtract_visibility_mock.return_value = observation

    cont_sub.stage_definition(
        upstream_output, False, "ps_out", True, "output_path"
    )

    logger_mock.warning.assert_called_once_with(
        "Cannot report extent of continuum subtraction."
    )
    fit_poly_on_vis_mock.assert_not_called()
    delayed_log_mock.assert_called_once()


def test_power_law_scaling():
    image = xr.DataArray(np.ones((4, 4)), dims=["x", "y"])
    frequencies = 100e6 + np.arange(10) * 25e6
    reference_frequency = 100e6
    spectral_index = 0.75

    expected_scaling = [
        1.0,
        0.845897,
        0.737788,
        0.657236,
        0.594604,
        0.544331,
        0.502973,
        0.468274,
        0.438691,
        0.413131,
    ]
    expected_data = np.ones((4, 4, 10)) * expected_scaling
    expected_image = xr.DataArray(expected_data, dims=["x", "y", "frequency"])

    actual_image = apply_power_law_scaling(
        image, frequencies, reference_frequency, spectral_index
    )

    np.testing.assert_array_almost_equal(actual_image, expected_image)


def test_power_law_scaling_throw_exception_when_missing_ref_freq():
    image = xr.DataArray(np.ones((4, 4)), dims=["x", "y"])
    frequencies = 100e6 + np.arange(10) * 25e6
    reference_frequency = None
    spectral_index = 0.75

    with pytest.raises(Exception):
        apply_power_law_scaling(
            image, frequencies, reference_frequency, spectral_index
        )


def test_power_law_scaling_when_image_has_frequency_dim_as_one():
    image = xr.DataArray(np.ones((4, 4, 1)), dims=["x", "y", "frequency"])
    frequencies = 100e6 + np.arange(10) * 25e6
    reference_frequency = 100e6
    spectral_index = 0.75

    expected_scaling = [
        1.0,
        0.845897,
        0.737788,
        0.657236,
        0.594604,
        0.544331,
        0.502973,
        0.468274,
        0.438691,
        0.413131,
    ]
    expected_data = np.ones((4, 4, 10)) * expected_scaling
    expected_image = xr.DataArray(expected_data, dims=["x", "y", "frequency"])

    actual_image = apply_power_law_scaling(
        image, frequencies, reference_frequency, spectral_index
    )

    np.testing.assert_array_almost_equal(actual_image, expected_image)


def test_power_law_scaling_freq_present_ref_freq_not_passed():
    """
    Assert power law scaling works when passing continuum image with freq = 1
    column and reference frequency is not passed
    """
    image = xr.DataArray(
        np.ones((4, 4, 1)),
        dims=["x", "y", "frequency"],
        coords=dict(frequency=np.array([100e6])),
    )
    frequencies = 100e6 + np.arange(10) * 25e6
    reference_frequency = None
    spectral_index = 0.75

    expected_scaling = [
        1.0,
        0.845897,
        0.737788,
        0.657236,
        0.594604,
        0.544331,
        0.502973,
        0.468274,
        0.438691,
        0.413131,
    ]
    expected_data = np.ones((4, 4, 10)) * expected_scaling
    expected_image = xr.DataArray(expected_data, dims=["x", "y", "frequency"])

    actual_image = apply_power_law_scaling(
        image, frequencies, reference_frequency, spectral_index
    )

    np.testing.assert_array_almost_equal(actual_image, expected_image)


def test_power_law_scaling_should_not_happen_when_frequency_present():
    image = xr.DataArray(
        np.ones((4, 4, 10), dtype=np.float32), dims=["x", "y", "frequency"]
    )
    frequencies = 100e6 + np.arange(10) * 25e6
    reference_frequency = 100e6
    spectral_index = 0.75

    actual_image = apply_power_law_scaling(
        image, frequencies, reference_frequency, spectral_index
    )

    np.testing.assert_array_almost_equal(actual_image, image)


def test_get_dask_array_from_fits():
    with pytest.raises(NotImplementedError):
        get_dask_array_from_fits("image_path", 0, (16, 16), np.float32, (4, 4))
