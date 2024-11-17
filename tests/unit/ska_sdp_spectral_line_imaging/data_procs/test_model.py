# pylint: disable=no-member

import mock
import numpy as np
import pytest
import xarray as xr

from ska_sdp_spectral_line_imaging.data_procs.model import (
    apply_power_law_scaling,
    fit_polynomial_on_visibility,
    report_peak_visibility,
)


@mock.patch("ska_sdp_spectral_line_imaging.data_procs.model.logger")
def test_should_report_peak_channel_value(logger_mock):

    visibility = xr.DataArray(
        [[[[1 + 1j, 2 + 1j, 3 + 1j], [4 + 1j, 0 + 1j, 4 + 3j]]]],
        dims=["time", "baseline_id", "polarization", "frequency"],
        coords=dict(frequency=[1, 2, 3]),
    )

    report_peak_visibility(visibility, "Hz").compute()

    logger_mock.info.assert_called_once_with(
        "Peak visibility Channel: 2."
        " Frequency: 3.0 Hz."
        " Peak Visibility: 5.0",
    )


@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.model."
    "np.polynomial.polynomial.polyfit"
)
@mock.patch("ska_sdp_spectral_line_imaging.data_procs.model.dask.delayed")
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

    result = fit_polynomial_on_visibility(array)

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
    "ska_sdp_spectral_line_imaging.data_procs.model."
    "np.polynomial.polynomial.polyfit"
)
@mock.patch("ska_sdp_spectral_line_imaging.data_procs.model.dask.delayed")
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

    fit_polynomial_on_visibility(array)

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
