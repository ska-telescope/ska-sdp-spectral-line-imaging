# pylint: disable=no-member
import numpy as np
from mock import MagicMock, Mock, call, mock

from ska_sdp_spectral_line_imaging.data_procs.imaging import (
    chunked_imaging,
    clean_cube,
    cube_imaging,
    generate_psf_image,
    image_ducc,
)


@mock.patch("ska_sdp_spectral_line_imaging.data_procs.imaging.wgridder")
def test_should_be_able_to_grid_visibilities(wgridder_mock):
    vis = Mock(spec=np.array(()), name="vis.np.array")
    vis.reshape.return_value = vis
    vis.astype.return_value = "RESHAPED_VIS_COMP64"
    flag = Mock(spec=np.array(()), name="flag.np.array")
    flag.reshape.return_value = "RESHAPED_FLAG"
    uvw = Mock(spec=np.array(()), name="uvw.np.array")
    uvw.reshape.return_value = "RESHAPED_UVW"
    freq = Mock(spec=np.array(()), name="freq.np.array")
    freq.reshape.return_value = "RESHAPED_FREQ"
    weight = Mock(spec=np.array(()), name="weight.np.array")
    weight.reshape.return_value = weight
    weight.astype.return_value = "RESHAPED_WEIGHT_FLT32"

    wgridder_mock.ms2dirty.return_value = "dirty_image"

    cell_size = 0.001
    nchan = 1
    ntime = 10
    nbaseline = 6
    epsilon = 1e-4
    nx = 256
    ny = 256

    output = image_ducc(
        weight,
        flag,
        uvw,
        freq,
        vis,
        cell_size,
        nx,
        ny,
        epsilon,
        nchan,
        ntime,
        nbaseline,
    )

    vis.reshape.assert_called_once_with(60, 1)
    vis.astype.assert_called_once_with(np.complex64)
    uvw.reshape.assert_called_once_with(60, 3)
    weight.reshape.assert_called_once_with(60, 1)
    weight.astype.assert_called_once_with(np.float32)
    freq.reshape.assert_called_once_with(1)
    wgridder_mock.ms2dirty.assert_called_once_with(
        "RESHAPED_UVW",
        "RESHAPED_FREQ",
        "RESHAPED_VIS_COMP64",
        "RESHAPED_WEIGHT_FLT32",
        256,
        256,
        0.001,
        0.001,
        0,
        0,
        1e-4,
        nthreads=1,
    )
    assert output == "dirty_image"


@mock.patch("ska_sdp_spectral_line_imaging.data_procs.imaging.xr")
def test_should_apply_image_ducc_on_data(xr_mock):
    image_vec = MagicMock(name="image_vec")
    xr_mock.apply_ufunc.return_value = image_vec
    image_vec.__truediv__.return_value = "cube_image"

    ps = MagicMock(name="ps")
    ps.time.size = 10
    ps.baseline_id.size = 10
    ps.WEIGHT.sum.return_value = "sum_of_weights"

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
    ps.WEIGHT.sum.assert_called_once_with(dim=["time", "baseline_id"])
    image_vec.__truediv__.assert_called_once_with("sum_of_weights")
    assert cube_image == "cube_image"


@mock.patch("ska_sdp_spectral_line_imaging.data_procs.imaging.np")
@mock.patch("ska_sdp_spectral_line_imaging.data_procs.imaging.Image")
@mock.patch("ska_sdp_spectral_line_imaging.data_procs.imaging.chunked_imaging")
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


@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.dask.array",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.xr",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.cube_imaging",
)
def test_should_generate_psf_image(
    cube_imaging_mock, xr_mock, dask_array_mock
):
    dask_array_mock.ones_like.return_value = "ones_array"
    xr_mock.DataArray.return_value = "vis_xdr"
    ps = MagicMock(name="ps")
    psf_ps = MagicMock(name="psf_ps")
    ps.assign.return_value = psf_ps

    psf = generate_psf_image(
        ps, 123, 1, 1, 0.0001, "wcs", "polarization_frame"
    )

    dask_array_mock.ones_like.assert_called_once_with(ps.VISIBILITY.data)
    xr_mock.DataArray.assert_called_once_with(
        "ones_array",
        attrs=ps.VISIBILITY.attrs,
        coords=ps.VISIBILITY.coords,
    )
    ps.assign.assert_called_once_with({"VISIBILITY": "vis_xdr"})
    cube_imaging_mock.assert_called_once_with(
        psf_ps, 123, 1, 1, 0.0001, "wcs", "polarization_frame"
    )
    assert psf == cube_imaging_mock.return_value


@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.deconvolve",
)
@mock.patch("ska_sdp_spectral_line_imaging.data_procs.imaging.restore_cube")
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.cube_imaging",
)
def test_should_generate_dirty_image_if_niter_major_is_zero(
    cube_imaging_mock,
    restore_cube_mock,
    deconvolve_mock,
):
    ps = MagicMock(name="ps")
    dirty_image = Mock(name="dirty_image")
    cube_imaging_mock.return_value = dirty_image
    gridding_params = {
        "epsilon": 1,
        "cell_size": 123,
        "image_size": 1,
        "scaling_factor": 2.0,
        "nx": 1234,
        "ny": 4567,
    }
    deconvolution_params = {}
    psf_image_path = "path_to_psf"
    n_iter_major = 0

    imaging_products = clean_cube(
        ps,
        psf_image_path,
        n_iter_major,
        gridding_params,
        deconvolution_params,
        "polarization_frame",
        "wcs",
        "beam_info",
    )

    cube_imaging_mock.assert_called_once_with(
        ps, 123, 1234, 4567, 1, "wcs", "polarization_frame"
    )
    deconvolve_mock.assert_not_called()

    restore_cube_mock.assert_not_called()

    assert imaging_products["dirty"] == dirty_image


@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.subtract_visibility"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.predict_for_channels",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.import_image_from_fits"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.Image",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.deconvolve",
)
@mock.patch("ska_sdp_spectral_line_imaging.data_procs.imaging.restore_cube")
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.cube_imaging",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.dask.array",
)
def test_should_generate_restored_image_and_other_imaging_products(
    dask_array_mock,
    cube_imaging_mock,
    restore_cube_mock,
    deconvolve_mock,
    image_mock,
    import_image_from_fits_mock,
    predict_mock,
    subtract_mock,
):
    dask_array_mock.zeros_like.return_value = "zeros_array"

    ps = MagicMock(name="ps")
    model_ps = MagicMock(name="model_ps")
    residual_ps = MagicMock(name="residual_ps")
    ps.copy.return_value = residual_ps
    residual_ps.assign.return_value = model_ps
    residual_ps1 = MagicMock(name="residual_ps1")
    residual_ps1.assign.return_value = model_ps
    residual_ps2 = MagicMock(name="residual_ps2")
    subtract_mock.side_effect = [residual_ps1, residual_ps2]

    model_visibility = MagicMock(name="model_visibility")
    predict_mock.return_value = model_visibility
    model_visibility.assign_attrs.return_value = model_visibility

    dirty_image = Mock(name="dirty_image")
    dirty_image.pixels.data = np.array([1])
    residual_image1 = Mock(name="residual_image1")
    residual_image2 = Mock(name="residual_image2")
    cube_imaging_mock.side_effect = [
        dirty_image,
        residual_image1,
        residual_image2,
    ]
    model_image = MagicMock(name="model image")
    image_mock.constructor.return_value = model_image
    model_image.assign.return_value = model_image
    # TODO: Remove this once polarization naming issue is fixed
    model_image.coords = []

    model_image_iter1 = Mock(name="model image per iteration 1")
    model_image_iter2 = Mock(name="model image per iteration 2")
    deconvolve_mock.side_effect = [
        (model_image_iter1, ""),
        (model_image_iter2, ""),
    ]

    restored_image = MagicMock(name="restored image")
    restore_cube_mock.return_value = restored_image

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

    n_iter_major = 2

    imaging_products = clean_cube(
        ps,
        psf_image_path,
        n_iter_major,
        gridding_params,
        deconvolution_params,
        "polarization_frame",
        "wcs",
        "beam_info",
    )

    import_image_from_fits_mock.assert_called_once_with(
        "path_to_psf", fixpol=True
    )

    # TODO: Add assert for empty image constructor
    # image_mock.constructor.assert_called_once_with()
    dask_array_mock.zeros_like.assert_called_once_with(dirty_image.pixels.data)
    image_mock.constructor.assert_called_once_with(
        data="zeros_array",
        polarisation_frame="polarization_frame",
        wcs="wcs",
    )

    deconvolve_mock.assert_has_calls(
        [
            call(
                dirty_image, psf_image, **gridding_params, param1=1, param2=2
            ),
            call(
                residual_image1,
                psf_image,
                **gridding_params,
                param1=1,
                param2=2,
            ),
        ]
    )
    model_image.assign.assert_has_calls(
        [
            call({"pixels": model_image.pixels + model_image_iter1.pixels}),
            call({"pixels": model_image.pixels + model_image_iter2.pixels}),
        ]
    )
    cube_imaging_mock.assert_has_calls(
        [
            call(ps, 123, 1234, 4567, 1, "wcs", "polarization_frame"),
            call(
                residual_ps1, 123, 1234, 4567, 1, "wcs", "polarization_frame"
            ),
            call(
                residual_ps2, 123, 1234, 4567, 1, "wcs", "polarization_frame"
            ),
        ]
    )
    predict_mock.assert_has_calls(
        [
            call(residual_ps, model_image.pixels, 1, 123),
            call(residual_ps1, model_image.pixels, 1, 123),
        ]
    )
    residual_ps.assign.assert_called_once_with(
        {"VISIBILITY": model_visibility}
    )
    residual_ps1.assign.assert_called_once_with(
        {"VISIBILITY": model_visibility}
    )
    subtract_mock.assert_has_calls(
        [
            call(ps, model_ps),
            call(ps, model_ps),
        ]
    )
    model_visibility.assign_attrs.assert_has_calls(
        [
            call(residual_ps.VISIBILITY.attrs),
            call(residual_ps1.VISIBILITY.attrs),
        ]
    )
    restore_cube_mock.assert_called_once_with(
        model_image, psf_image, residual_image2, "beam_info"
    )

    assert imaging_products["model"] == model_image
    assert imaging_products["psf"] == psf_image
    assert imaging_products["residual"] == residual_image2
    assert imaging_products["restored"] == restored_image


@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.subtract_visibility"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.predict_for_channels",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.import_image_from_fits"
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.Image",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.deconvolve",
)
@mock.patch("ska_sdp_spectral_line_imaging.data_procs.imaging.restore_cube")
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.cube_imaging",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.xr.DataArray",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.dask.array",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.data_procs.imaging.generate_psf_image",
)
def test_should_create_psf_if_psf_is_none(
    generate_psf_image_mock,
    dask_array_mock,
    data_array_mock,
    cube_imaging_mock,
    restore_cube_mock,
    deconvolve_mock,
    image_mock,
    import_image_from_fits_mock,
    predict_mock,
    subtract_mock,
):
    ps = MagicMock(name="ps")

    dirty_image = Mock(name="dirty_image")
    residual_image = MagicMock(name="residual_image")
    cube_imaging_mock.side_effect = [dirty_image, residual_image]

    model_image = MagicMock(name="model image")
    image_mock.constructor.return_value = model_image
    model_image.assign.return_value = model_image
    # TODO: Remove this once polarization naming issue is fixed
    model_image.coords = []

    # model_image_iter = Mock(name="model image per iteration")
    gen_psf_image = MagicMock(name="gen_psf_image")
    generate_psf_image_mock.return_value = gen_psf_image
    deconvolve_mock.return_value = (Mock(name="model_image_iter"), "")

    gridding_params = {
        "epsilon": 0.0001,
        "cell_size": 123,
        "image_size": 1,
        "scaling_factor": 2.0,
        "nx": 1,
        "ny": 1,
    }
    deconvolution_params = {}
    psf_image_path = None
    n_iter_major = 1

    clean_cube(
        ps,
        psf_image_path,
        n_iter_major,
        gridding_params,
        deconvolution_params,
        "polarization_frame",
        "wcs",
        "beam_info",
    )

    generate_psf_image_mock.assert_called_once_with(
        ps, 123, 1, 1, 0.0001, "wcs", "polarization_frame"
    )

    deconvolve_mock.assert_called_once_with(
        dirty_image,
        gen_psf_image,
        **gridding_params,
        **deconvolution_params,
    )

    restore_cube_mock.assert_called_once_with(
        model_image,
        gen_psf_image,
        residual_image,
        "beam_info",
    )
