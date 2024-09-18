from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stubs.imaging import (
    chunked_imaging,
    cube_imaging,
    image_ducc,
)


@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.xr")
def test_should_apply_image_ducc_on_data(xr_mock):
    image_vec = Mock(name="image_vec")
    image_vec.data = Mock(name="data")
    xr_mock.apply_ufunc.return_value = image_vec
    xr_mock.DataArray.return_value = "cube_image"

    ps = Mock(name="ps")
    ps.WEIGHT = Mock(name="weights")
    ps.UVW = Mock(name="uvw")
    ps.FLAG = Mock(name="flag")
    ps.frequency = Mock(name="frequency")
    ps.VISIBILITY = Mock(name="VISIBILITY")
    ps.time.size = 10
    ps.baseline_id.size = 10

    cell_size = 16
    nx = 256
    ny = 256
    epsilon = 0

    cube_image = chunked_imaging(ps, cell_size, nx, ny, epsilon)

    xr_mock.apply_ufunc.assert_called_once_with(
        image_ducc,
        ps.WEIGHT,
        ps.FLAG,
        ps.UVW,
        ps.frequency,
        ps.VISIBILITY,
        input_core_dims=[
            ["time", "baseline_id"],
            ["time", "baseline_id"],
            ["time", "baseline_id", "uvw_label"],
            [],
            ["time", "baseline_id"],
        ],
        output_core_dims=[["ra", "dec"]],
        vectorize=True,
        kwargs=dict(
            nchan=1,
            ntime=10,
            nbaseline=10,
            cell_size=16,
            epsilon=0,
            nx=256,
            ny=256,
        ),
    )

    xr_mock.DataArray.assert_called_once_with(
        image_vec.data, dims=["frequency", "polarization", "ra", "dec"]
    )

    assert cube_image == "cube_image"


@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.xr")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.PolarisationFrame",
    return_value="pol_frame",
)
@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.Image")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.get_wcs", return_value="WCS"
)
def test_should_perform_cube_imaging(
    mock_get_wcs, mock_image, mock_pol_frame, mock_xr
):
    image_vec = Mock(name="image_vec")
    image_vec.data = Mock(name="data")
    mock_image.constructor.return_value = "cube_image"

    ps = Mock(name="ps")
    ps.polarization.data = ["XX", "YY"]
    ps.time.size = 10
    ps.chunksizes = {"a": 1}
    ps.sizes = {"frequency": 1, "polarization": 2}
    ps.baseline_id.size = 10

    mock_xr.map_blocks.return_value = image_vec

    cell_size = 0
    nx = 256
    ny = 256

    cube_image = cube_imaging(ps, cell_size, nx, ny, 1e-4)

    mock_xr.map_blocks.assert_called_once_with(
        chunked_imaging,
        ps,
        template=mock_xr.DataArray().chunk(),
        kwargs={"nx": nx, "ny": ny, "epsilon": 1e-4, "cell_size": 0.0},
    )

    mock_pol_frame.assert_called_once_with("linearnp")
    mock_get_wcs.assert_called_once_with(ps, cell_size, nx, ny)
    mock_image.constructor.assert_called_once_with(
        data=image_vec.data, polarisation_frame="pol_frame", wcs="WCS"
    )

    assert cube_image == "cube_image"
