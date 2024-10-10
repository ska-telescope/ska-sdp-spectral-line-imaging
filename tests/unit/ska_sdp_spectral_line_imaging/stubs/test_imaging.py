# pylint: disable=no-member
import numpy as np
import pytest
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
    image_vec.__truediv__ = lambda x, y: "cube_image"

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
        output_core_dims=[["y", "x"]],
        vectorize=True,
        keep_attrs=True,
        dask="parallelized",
        output_dtypes=[np.float32],
        dask_gufunc_kwargs={
            "output_sizes": {"y": 256, "x": 256},
        },
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

    # image_vec.__truediv__.assert_called_once_with(ps.WEIGHT)

    assert cube_image == "cube_image"


@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.np")
@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.Image")
@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.chunked_imaging")
def test_should_perform_cube_imaging(
    mock_chunked_imaging, mock_image, mock_numpy
):
    image_vec = Mock(name="image_vec")
    image_vec.data = Mock(name="data")
    mock_chunked_imaging.return_value = image_vec
    mock_image.constructor.return_value = "cube_image"
    mock_numpy.deg2rad.return_value = 0.5

    ps = Mock(name="ps")
    cell_size = 7200
    nx = 123
    ny = 456

    cube_image = cube_imaging(ps, cell_size, nx, ny, 1e-4, "WCS", "pol_frame")

    mock_numpy.deg2rad.assert_called_once_with(2)
    mock_chunked_imaging.assert_called_once_with(
        ps,
        nx=123,
        ny=456,
        epsilon=1e-4,
        cell_size=0.5,
    )
    mock_image.constructor.assert_called_once_with(
        data=image_vec.data, polarisation_frame="pol_frame", wcs="WCS"
    )
    assert cube_image == "cube_image"


@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.subtract_visibility")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.predict_for_channels",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.import_image_from_fits"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.Image",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.deconvolve_cube",
)
@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.restore_cube")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stubs.imaging.cube_imaging",
)
def test_should_perform_major_cyle(
    cube_imaging_mock,
    restore_cube_mock,
    deconvolve_mock,
    image_mock,
    import_image_from_fits_mock,
    predict_mock,
    subtract_mock,
):
    ps = Mock(name="ps")
    residual_ps = Mock(name="residual_ps")
    ps.assign = Mock(name="assign", return_value=residual_ps)
    subtract_mock.return_value = residual_ps
    predicted_visibilities = Mock(name="predicted_visibilities")
    predict_mock.return_value = predicted_visibilities
    predicted_visibilities.assign_attrs.return_value = predicted_visibilities

    dirty_image1 = Mock(name="dirty_image1")
    dirty_image2 = Mock(name="dirty_image2")
    cube_imaging_mock.side_effect = [dirty_image2]

    dirty_image1.pixels.data = np.array([1, 2])
    dirty_image1.image_acc.polarisation_frame = "polarization_frame"
    dirty_image1.image_acc.wcs = "wcs"

    model_image = Mock(name="model image")
    image_mock.constructor.return_value = model_image
    model_image.pixels.__add__ = lambda x, y: model_image.pixels
    model_image.assign.return_value = model_image
    # TODO: Remove this once polarization naming issue is fixed
    model_image.coords = []

    model_image_iter = Mock(name="model image per iteration")
    deconvolve_mock.return_value = [model_image_iter, "residual_image"]

    gridding_params = {
        "epsilon": 1,
        "cell_size": 123,
        "image_size": 1,
        "scaling_factor": 2.0,
        "nx": 1234,
        "ny": 4567,
    }
    deconvolution_params = {"param1": 1, "param2": 2}

    psf_image_path = "path_to_psf"
    psf_image = Mock(name="psf_image")
    # TODO: Remove this once psf issue is solved
    psf_image.assign_coords.return_value = psf_image

    import_image_from_fits_mock.return_value = psf_image

    n_iter_major = 1

    clean_cube(
        ps,
        psf_image_path,
        dirty_image1,
        n_iter_major,
        gridding_params,
        deconvolution_params,
        "polarization_frame",
        "wcs",
    )

    import_image_from_fits_mock.assert_called_once_with(
        "path_to_psf", fixpol=True
    )

    # TODO: Add assert for empty image constructor
    # image_mock.constructor.assert_called_once_with()

    deconvolve_mock.assert_has_calls(
        [
            call(dirty_image1, psf_image, param1=1, param2=2),
            call(dirty_image2, psf_image, param1=1, param2=2),
        ]
    )

    model_image.assign.assert_has_calls(
        [
            call({"pixels": model_image.pixels + model_image_iter.pixels}),
            call({"pixels": model_image.pixels + model_image_iter.pixels}),
        ]
    )

    cube_imaging_mock.assert_has_calls(
        [
            call(residual_ps, 123, 1234, 4567, 1, "wcs", "polarization_frame"),
        ]
    )

    predict_mock.assert_called_once_with(ps, model_image.pixels, 1, 123)
    subtract_mock.assert_called_once_with(ps, residual_ps)
    predicted_visibilities.assign_attrs.assert_called_once_with(
        ps.VISIBILITY.attrs
    )

    restore_cube_mock.assert_called_once_with(
        model_image, psf_image, "residual_image"
    )

    ps.assign.assert_called_once_with({"VISIBILITY": predicted_visibilities})


@mock.patch("ska_sdp_spectral_line_imaging.stubs.imaging.cube_imaging")
def test_should_throw_non_implemented_error_if_psf_is_none(cube_imaging_mock):

    ps = Mock(name="ps")

    gridding_params = {
        "epsilon": 0.0001,
        "cell_size": 123,
        "image_size": 1,
        "scaling_factor": 2.0,
        "nx": 1,
        "ny": 1,
    }
    deconvolution_params = {"param1": 1, "param2": 2}
    psf_image_path = None
    n_iter_major = 2

    with pytest.raises(NotImplementedError):
        clean_cube(
            ps,
            psf_image_path,
            "dirty image",
            n_iter_major,
            gridding_params,
            deconvolution_params,
            "polarization frame",
            "wcs",
        )
