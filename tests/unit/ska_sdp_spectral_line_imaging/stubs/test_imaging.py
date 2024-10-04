from mock import Mock, call, mock

from ska_sdp_spectral_line_imaging.stubs.imaging import (
    chunked_imaging,
    clean_cube,
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


@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.subtract_visibility")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.predict_for_channels",
    return_value="predicted_visibilities",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.deconvolve_cube",
    return_value=["model_image", "residual_image"],
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.cube_imaging",
    side_effect=["cube_image1", "cube_image2"],
)
def test_should_perform_major_cyle(
    cube_imaging_mock, deconvolve_mock, predict_mock, subtract_mock
):

    ps = Mock(name="ps")
    residual_ps = Mock(name="residual_ps")
    ps.assign = Mock(name="assign", return_value=residual_ps)
    subtract_mock.return_value = residual_ps

    gridding_params = {
        "epsilon": 0.0001,
        "cell_size": 123,
        "image_size": 1,
        "scaling_factor": 2.0,
        "nx": 1,
        "ny": 1,
    }
    deconvolution_params = {"param1": 1, "param2": 2}
    psf_image = "psf_image"
    n_iter_major = 2

    clean_cube(
        ps, psf_image, n_iter_major, gridding_params, deconvolution_params
    )

    cube_imaging_mock.assert_has_calls(
        [call(ps, 123, 1, 1, 0.0001), call(residual_ps, 123, 1, 1, 0.0001)]
    )
    deconvolve_mock.assert_has_calls(
        [
            call("cube_image1", "psf_image", param1=1, param2=2),
            call("cube_image2", "psf_image", param1=1, param2=2),
        ]
    )
    predict_mock.assert_called_once_with(ps, "model_image", 0.0001, 123)
    subtract_mock.assert_called_once_with(ps, residual_ps)
    ps.assign.assert_called_once_with({"VISIBILITY": "predicted_visibilities"})
