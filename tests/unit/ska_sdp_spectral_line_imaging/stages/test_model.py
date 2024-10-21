import mock
import numpy as np
import pytest
import xarray as xr
from mock import MagicMock, Mock, patch

from ska_sdp_spectral_line_imaging.stages.model import read_model
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput

from ska_sdp_spectral_line_imaging.stages.model import cont_sub
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


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


@pytest.fixture(scope="function")
def mock_ps():
    ps = Mock(name="ps")
    ps.chunksizes = {
        "polarization": 2,
        "frequency": 2,
        "baseline_id": 15,
        "time": 30,
    }
    ps.polarization = ["I", "V"]
    ps.frequency = [1.0, 2.0, 3.0]

    yield ps


@patch("ska_sdp_spectral_line_imaging.stages.model." "fits.open")
def test_should_read_model_from_spectral_image(fits_open_mock, mock_ps):
    pols = ["I", "V"]

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

    upstrem_output = read_model.stage_definition(
        up_out, image, image_type, pols
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
def test_should_read_model_from_continuum_image(fits_open_mock, mock_ps):
    pols = ["I", "V"]

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

    upstrem_output = read_model.stage_definition(
        up_out, image, image_type, pols
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
    pols = [""]
    up_out = UpstreamOutput()
    image = ""
    image_type = "invalid"

    with pytest.raises(AttributeError) as err:
        read_model.stage_definition(up_out, image, image_type, pols)

    assert "image_type must be spectral or continuum" in str(err.value)
