import numpy as np
from mock import MagicMock, Mock, call, patch

from ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image import (
    convert_clean_beam_to_pixels,
    fit_psf,
    restore_channel,
    restore_cube,
)


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "Image.constructor"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "restore_channel"
)
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image." "xr")
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "Gaussian2DKernel"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image"
    ".convert_clean_beam_to_pixels"
)
def test_should_restore_cube(
    clean_beam_mock,
    gaussian_kernel_mock,
    xr_mock,
    restore_channel_mock,
    image_con_mock,
):
    model_image = Mock(name="model image")
    model_image.image_acc.wcs.wcs.cdelt = [10, 90]
    residual_image = Mock(name="residual image")
    psf_image = Mock(name="psf image")

    image_mock = Mock(name="Image")
    xr_restored = Mock(name="Image")
    pixel_added_restored = Mock(name="Image")

    get_item_mock = Mock(name="get_item_mock")
    get_item_mock.return_value = get_item_mock
    residual_image.__getitem__ = get_item_mock
    model_image.__getitem__ = get_item_mock

    image_con_mock.return_value = image_mock

    clean_beam_mock.return_value = ["x", "y", "theta"]
    xr_mock.DataArray.return_value = "beam_pixels_arr"

    gaussian_kernel_mock.return_value = gaussian_kernel_mock
    xr_mock.apply_ufunc.return_value = xr_restored
    xr_restored.__add__ = Mock(name="Add", return_value=pixel_added_restored)

    image_mock.attrs.__setitem__ = Mock(name="set_item")

    restored_image = restore_cube(
        model_image, psf_image, residual_image, {"bmaj": 0.1}
    )

    clean_beam_mock.assert_called_once_with(np.pi / 2, {"bmaj": 0.1})

    # gaussian_kernel_mock.assert_called_once_with(
    #     x_stddev="x",
    #     y_stddev="y",
    #     theta="theta",
    # )

    # gaussian_kernel_mock.normalize.assert_called_once_with(mode="peak")
    xr_mock.DataArray.assert_called_once_with(
        ["x", "y", "theta"], dims=["beam"]
    )

    xr_mock.apply_ufunc.assert_called_once_with(
        restore_channel_mock,
        get_item_mock,
        "beam_pixels_arr",
        input_core_dims=[["y", "x"], ["beam"]],
        output_core_dims=[["y", "x"]],
        # TODO: parameterize dtype
        output_dtypes=(np.float32),
        vectorize=True,
        dask="parallelized",
        keep_attrs=True,
    )

    xr_restored.__add__.assert_called_once_with(residual_image["pixels"])

    image_con_mock.assert_called_once_with(
        data=pixel_added_restored.data,
        polarisation_frame=model_image.image_acc.polarisation_frame,
        wcs=model_image.image_acc.wcs,
    )

    image_mock.attrs.__setitem__.assert_called_once_with(
        "clean_beam", {"bmaj": 0.1}
    )

    assert restored_image == image_mock


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "Image.constructor"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "xr.apply_ufunc"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "Gaussian2DKernel"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image"
    ".convert_clean_beam_to_pixels"
)
def test_should_restore_cube_without_adding_residual(
    clean_beam_mock,
    gaussian_kernel_mock,
    apply_ufunc_mock,
    image_con_mock,
):
    model_image = Mock(name="model image")
    model_image.image_acc.wcs.wcs.cdelt = [10, 90]
    psf_image = Mock(name="psf image")
    image_mock = Mock(name="Image")
    xr_restored = MagicMock(name="Image")

    get_item_mock = Mock(name="get_item_mock")
    get_item_mock.return_value = get_item_mock
    model_image.__getitem__ = get_item_mock

    image_con_mock.return_value = image_mock
    clean_beam_mock.return_value = ["x", "y", "theta"]
    gaussian_kernel_mock.return_value = gaussian_kernel_mock
    apply_ufunc_mock.return_value = xr_restored

    image_mock.attrs.__setitem__ = Mock(name="set_item")

    restored_image = restore_cube(model_image, psf_image, None, {"bmaj": 10})

    xr_restored.__add__.assert_not_called()

    assert restored_image == image_mock
    image_con_mock.assert_called_once_with(
        data=xr_restored.data,
        polarisation_frame=model_image.image_acc.polarisation_frame,
        wcs=model_image.image_acc.wcs,
    )


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "Image.constructor"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "restore_channel"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "xr.apply_ufunc"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "Gaussian2DKernel"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image"
    ".convert_clean_beam_to_pixels"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image"
    ".fit_psf"
)
def test_should_restore_cube_when_beam_info_is_none(
    fit_psf_mock,
    clean_beam_mock,
    gaussian_kernel_mock,
    apply_ufunc_mock,
    restore_channel_mock,
    image_con_mock,
):
    model_image = Mock(name="model image")
    model_image.image_acc.wcs.wcs.cdelt = [10, 90]
    psf_image = Mock(name="psf image")
    image_mock = MagicMock(name="Image")
    xr_restored = MagicMock(name="Image")

    get_item_mock = Mock(name="get_item_mock")
    get_item_mock.return_value = get_item_mock
    model_image.__getitem__ = get_item_mock
    psf_image.__getitem__ = get_item_mock

    image_con_mock.return_value = image_mock
    clean_beam_mock.return_value = ["x", "y", "theta"]
    gaussian_kernel_mock.return_value = gaussian_kernel_mock
    apply_ufunc_mock.side_effect = ["beam_pixels", xr_restored]

    image_mock.attrs.__setitem__ = Mock(name="set_item")

    restored_image = restore_cube(model_image, psf_image, None, {"bmaj": None})

    assert restored_image == image_mock

    apply_ufunc_mock.assert_has_calls(
        [
            call(
                fit_psf_mock,
                get_item_mock,
                input_core_dims=[
                    ["y", "x"],
                ],
                output_core_dims=[["beam"]],
                dask_gufunc_kwargs={
                    "output_sizes": {
                        "beam": 3,
                    }
                },
                # TODO: parameterize dtype
                output_dtypes=(np.float64),
                vectorize=True,
                dask="parallelized",
                keep_attrs=True,
            ),
            call(
                restore_channel_mock,
                get_item_mock,
                "beam_pixels",
                input_core_dims=[["y", "x"], ["beam"]],
                output_core_dims=[["y", "x"]],
                # TODO: parameterize dtype
                output_dtypes=(np.float32),
                vectorize=True,
                dask="parallelized",
                keep_attrs=True,
            ),
        ]
    )

    image_mock.attrs.__setitem__.assert_not_called()

    image_con_mock.assert_called_once_with(
        data=xr_restored.data,
        polarisation_frame=model_image.image_acc.polarisation_frame,
        wcs=model_image.image_acc.wcs,
    )


def test_should_convert_clean_beam_to_pixels():
    cellsize = 1
    clean_beam = {"bmin": 0.3, "bmaj": 0.6, "bpa": 0.05}

    beam_pixels = convert_clean_beam_to_pixels(cellsize, clean_beam)

    np.allclose(beam_pixels, [0.00222352, 0.00444704, 0.00087266])


def test_should_generate_beam_pixels_by_fitting_psf():
    psf = np.arange(64 * 64).reshape(64, 64)

    beam_pixels = fit_psf(psf)

    np.allclose(beam_pixels, [25.93199291, 24.74372649, 34.59370391])


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image"
    ".Gaussian2DKernel"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image"
    ".convolve_fft"
)
def test_should_restore_channel(convolve_fft_mock, gauss_kern_mock):
    model = Mock(name="model image")
    beam_pixels = [10, 20, 30]
    kernel = Mock(name="kernel")

    gauss_kern_mock.return_value = kernel
    convolve_fft_mock.return_value = "convolved_array"

    output = restore_channel(model, beam_pixels)

    gauss_kern_mock.assert_called_once_with(
        x_stddev=10,
        y_stddev=20,
        theta=30,
    )

    kernel.normalize.assert_called_once_with(mode="peak")

    convolve_fft_mock.assert_called_once_with(
        model,
        kernel,
        normalize_kernel=False,
        allow_huge=True,
        boundary="wrap",
    )

    assert output == "convolved_array"
