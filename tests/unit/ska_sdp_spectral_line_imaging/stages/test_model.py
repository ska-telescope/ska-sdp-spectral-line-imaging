import mock
import numpy as np
import pytest
import xarray as xr
from mock import MagicMock, Mock, call, patch

from ska_sdp_spectral_line_imaging.stages.model import (
    _fit_polynomial_on_visibility,
    cont_sub,
    read_model,
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


@pytest.fixture(scope="function")
def mock_ps():
    ps = Mock(name="ps")
    ps.chunksizes = {
        "polarization": 2,
        "frequency": 2,
        "baseline_id": 15,
        "time": 30,
    }
    ps.polarization = xr.DataArray(["I", "V"], dims="polarization")
    ps.frequency = [1.0, 2.0, 3.0]

    yield ps


@patch("ska_sdp_spectral_line_imaging.stages.model." "fits.open")
@patch("ska_sdp_spectral_line_imaging.stages.model." "os")
def test_should_read_model_from_spectral_image(
    os_mock, fits_open_mock, mock_ps
):
    up_out = UpstreamOutput()
    up_out["ps"] = mock_ps

    image = "/path/ws-%s-im.fits"
    image_type = "spectral"

    fits_hdu_0 = Mock(name="fits_hdu_0")
    fits_hdu_0.data = np.arange(12).reshape(1, 3, 2, 2)

    fits_hdu_I = [fits_hdu_0]
    fits_hdu_V = [fits_hdu_0]

    enter_mock = MagicMock()
    enter_mock.__enter__.side_effect = [fits_hdu_I, fits_hdu_V]
    fits_open_mock.return_value = enter_mock

    os_mock.path.exists.return_value = True

    expected_data = xr.DataArray(
        data=np.array(
            [
                [[[0, 1], [2, 3]], [[4, 5], [6, 7]], [[8, 9], [10, 11]]],
                [[[0, 1], [2, 3]], [[4, 5], [6, 7]], [[8, 9], [10, 11]]],
            ]
        ),
        dims=["polarization", "frequency", "y", "x"],
        coords={
            "frequency": [1.0, 2.0, 3.0],
            "polarization": ["I", "V"],
        },
    ).chunk(
        {
            "polarization": 2,
            "frequency": 2,
        }
    )

    upstrem_output = read_model.stage_definition(up_out, image, image_type)

    os_mock.path.exists.assert_has_calls(
        [
            mock.call("/path/ws-I-im.fits"),
            mock.call("/path/ws-V-im.fits"),
        ],
    )

    fits_open_mock.assert_has_calls(
        [
            mock.call("/path/ws-I-im.fits"),
            mock.call("/path/ws-V-im.fits"),
        ],
        any_order=True,
    )

    xr.testing.assert_equal(
        upstrem_output["model_image"], expected_data, check_dim_order=True
    )
    # additional test since chunksizes are not asserted above
    assert upstrem_output["model_image"].chunksizes == expected_data.chunksizes


@patch("ska_sdp_spectral_line_imaging.stages.model." "fits.open")
@patch("ska_sdp_spectral_line_imaging.stages.model." "os")
def test_should_read_model_from_continuum_image(
    os_mock, fits_open_mock, mock_ps
):
    up_out = UpstreamOutput()
    up_out["ps"] = mock_ps

    image = "/path/ws-%s-im.fits"
    image_type = "continuum"

    fits_hdu_0 = Mock(name="fits_hdu_0")
    fits_hdu_0.data = np.arange(12).reshape(1, 1, 3, 4)

    fits_hdu_I = [fits_hdu_0]
    fits_hdu_V = [fits_hdu_0]

    enter_mock = MagicMock()
    enter_mock.__enter__.side_effect = [fits_hdu_I, fits_hdu_V]
    fits_open_mock.return_value = enter_mock

    os_mock.path.exists.return_value = True

    expected_data = xr.DataArray(
        data=np.array(
            [
                [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]],
                [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]],
            ]
        ),
        dims=["polarization", "y", "x"],
        coords={
            "polarization": ["I", "V"],
        },
    ).chunk(
        {
            "polarization": 2,
        }
    )

    upstrem_output = read_model.stage_definition(up_out, image, image_type)

    os_mock.path.exists.assert_has_calls(
        [
            mock.call("/path/ws-I-im.fits"),
            mock.call("/path/ws-V-im.fits"),
        ],
    )

    fits_open_mock.assert_has_calls(
        [
            mock.call("/path/ws-I-im.fits"),
            mock.call("/path/ws-V-im.fits"),
        ],
        any_order=True,
    )

    xr.testing.assert_equal(
        upstrem_output["model_image"], expected_data, check_dim_order=True
    )
    # additional test since chunksizes are not asserted above
    assert upstrem_output["model_image"].chunksizes == expected_data.chunksizes


def test_should_raise_attribute_error_for_invalid_image_type():
    up_out = UpstreamOutput()
    image = "/path/ws-%s-im.fits"
    image_type = "invalid"

    with pytest.raises(AttributeError) as err:
        read_model.stage_definition(up_out, image, image_type)

    assert "image_type must be spectral or continuum" in str(err.value)


@patch("ska_sdp_spectral_line_imaging.stages.model." "os")
def test_should_raise_file_not_found_error_for_missing_fits_file(
    os_mock, mock_ps
):
    up_out = UpstreamOutput()
    up_out["ps"] = mock_ps

    image = "/path/ws-%s-im.fits"
    image_type = "spectral"

    os_mock.path.exists.side_effect = [True, False]

    with pytest.raises(FileNotFoundError) as err:
        read_model.stage_definition(up_out, image, image_type)

    os_mock.path.exists.assert_has_calls(
        [
            mock.call("/path/ws-I-im.fits"),
            mock.call("/path/ws-V-im.fits"),
        ],
    )

    assert (
        "FITS image /path/ws-V-im.fits corresponding to "
        "polarization V not found." in str(err.value)
    )
