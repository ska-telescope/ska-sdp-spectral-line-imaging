import numpy as np
from mock import Mock, patch

from ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image import (
    restore_cube,
)


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "Image.constructor"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "convolve_fft"
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
def test_should_restore_cube(
    clean_beam_mock,
    gaussian_kernel_mock,
    apply_ufunc_mock,
    convolve_fft_mock,
    image_con_mock,
):
    model_image = Mock(name="model image")
    residual_image = Mock(name="residual image")
    image_mock = Mock(name="Image")
    xr_restored = Mock(name="Image")
    pixel_added_restored = Mock(name="Image")

    get_item_mock = Mock(name="get_item_mock")
    get_item_mock.return_value = get_item_mock
    residual_image.__getitem__ = get_item_mock
    model_image.__getitem__ = get_item_mock

    image_con_mock.return_value = image_mock
    clean_beam_mock.return_value = ["x", "y", "theta"]
    gaussian_kernel_mock.return_value = gaussian_kernel_mock
    apply_ufunc_mock.return_value = xr_restored
    xr_restored.__add__ = Mock(name="Add", return_value=pixel_added_restored)

    image_mock.attrs.__setitem__ = Mock(name="set_item")

    restored_image = restore_cube(model_image, "clean_beam", residual_image)

    assert restored_image == image_mock
    clean_beam_mock.assert_called_once_with(model_image, "clean_beam")

    gaussian_kernel_mock.assert_called_once_with(
        x_stddev="x",
        y_stddev="y",
        theta="theta",
    )

    gaussian_kernel_mock.normalize.assert_called_once_with(mode="peak")
    apply_ufunc_mock.assert_called_once_with(
        convolve_fft_mock,
        get_item_mock,
        input_core_dims=[
            ["y", "x"],
        ],
        output_core_dims=[["y", "x"]],
        # TODO: parameterize dtype
        output_dtypes=(np.float32),
        vectorize=True,
        dask="parallelized",
        keep_attrs=True,
        kwargs=dict(
            kernel=gaussian_kernel_mock,
            normalize_kernel=False,
            allow_huge=True,
            boundary="wrap",
        ),
    )

    xr_restored.__add__.assert_called_once_with(residual_image["pixels"])

    image_con_mock.assert_called_once_with(
        data=pixel_added_restored.data,
        polarisation_frame=model_image.image_acc.polarisation_frame,
        wcs=model_image.image_acc.wcs,
    )

    image_mock.attrs.__setitem__.assert_called_once_with(
        "clean_beam", "clean_beam"
    )


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "Image.constructor"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.restore_image."
    "convolve_fft"
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
    convolve_fft_mock,
    image_con_mock,
):
    model_image = Mock(name="model image")
    image_mock = Mock(name="Image")
    xr_restored = Mock(name="Image")

    get_item_mock = Mock(name="get_item_mock")
    get_item_mock.return_value = get_item_mock
    model_image.__getitem__ = get_item_mock

    image_con_mock.return_value = image_mock
    clean_beam_mock.return_value = ["x", "y", "theta"]
    gaussian_kernel_mock.return_value = gaussian_kernel_mock
    apply_ufunc_mock.return_value = xr_restored

    image_mock.attrs.__setitem__ = Mock(name="set_item")

    restored_image = restore_cube(model_image, "clean_beam")

    assert restored_image == image_mock
    image_con_mock.assert_called_once_with(
        data=xr_restored.data,
        polarisation_frame=model_image.image_acc.polarisation_frame,
        wcs=model_image.image_acc.wcs,
    )
