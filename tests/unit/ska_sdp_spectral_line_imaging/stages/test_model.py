import mock
import numpy as np
import pytest
import xarray as xr
from mock import MagicMock, Mock, patch

from ska_sdp_spectral_line_imaging.stages.model import cont_sub, read_model
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
def test_should_perform_continuum_subtraction(subtract_visibility_mock):

    observation = Mock(name="observation")
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    observation.assign.return_value = "model"
    subtracted_vis = Mock(name="subtracted_vis")
    subtracted_vis.VISIBILITY.assign_attrs.return_value = "sub_vis_with_attrs"
    subtract_visibility_mock.return_value = subtracted_vis

    cont_sub.stage_definition(upstream_output, False)
    observation.assign.assert_called_once_with(
        {"VISIBILITY": observation.VISIBILITY_MODEL}
    )
    subtract_visibility_mock.assert_called_once_with(observation, "model")
    subtracted_vis.assign.assert_called_once_with(
        {"VISIBILITY": "sub_vis_with_attrs"}
    )


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
def test_should_not_report_peak_channel_value(
    numpy_mock, subtract_visibility_mock
):

    observation = Mock(name="observation")
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation

    cont_sub.stage_definition(upstream_output, False)

    numpy_mock.abs.assert_not_called()


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.logger")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
def test_should_report_peak_channel_value(
    numpy_mock, subtract_visibility_mock, logger_mock
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
    numpy_mock.values = "VALUES"

    cont_sub.stage_definition(upstream_output, True)

    numpy_mock.abs.assert_called_once_with(observation.VISIBILITY)
    numpy_mock.max.assert_called_once_with(
        dim=["time", "baseline_id", "polarization"]
    )
    numpy_mock.idxmax.assert_called_once()
    logger_mock.info.assert_called_once_with(
        "Peak visibility Channel: VALUES Hz"
    )


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
