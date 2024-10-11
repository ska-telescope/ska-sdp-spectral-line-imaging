import numpy as np
import pytest
from mock import Mock, patch

from ska_sdp_spectral_line_imaging.stubs.deconvolution import (
    deconvolve_cube,
    restore_cube,
)


@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.hogbom")
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.Image.constructor")
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.np.ones",
    return_value="np.ones",
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.np.allclose",
    return_value=True,
)
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.xr.apply_ufunc")
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.xr.DataArray")
def test_should_deconvolve_cube(
    mock_data_array,
    apply_ufunc_mock,
    np_all_close_mock,
    np_ones_mock,
    image_con_mock,
    hogbom_mock,
):
    input_image = Mock(name="input image")
    psf_image = Mock(name="psf image")
    component = Mock(name="component")
    residual = Mock(name="residual")
    mock_data_array.return_value = mock_data_array
    apply_ufunc_mock.return_value = (component, residual)
    image_con_mock.side_effect = ["component_image", "residual_image"]

    get_item_mock = Mock(name="get_item_mock")
    get_item_mock.return_value = get_item_mock
    input_image.__getitem__ = get_item_mock
    psf_image.__getitem__ = get_item_mock

    deconvolution_params = {
        "algorithm": "hogbom",
        "findpeak": "RASCIL",
        "fractional_threshold": 0.01,
        "gain": 0.1,
        "mask": None,
        "niter": 5,
        "nmoment": 3,
        "prefix": "",
        "scales": [0, 3, 10, 30],
        "threshold": 0.0,
        "window_shape": None,
    }

    component_image, residual_image = deconvolve_cube(
        input_image, psf_image, **deconvolution_params
    )

    assert component_image == "component_image"
    assert residual_image == "residual_image"

    mock_data_array.assert_called_once_with(
        "np.ones",
        dims=input_image["pixels"].dims,
        coords=input_image["pixels"].coords,
    )


@pytest.mark.parametrize(
    "algorithm",
    ["msclean", "msmfsclean", "mfsmsclean", "mmclean", "hogbom-complex"],
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.np.allclose",
    return_value=True,
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.np.ones",
    return_value="np.ones",
)
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.xr.DataArray")
def test_should_throw_exceptions_for_non_implemented_algorithms(
    mock_data_array, np_ones_mock, np_all_close_mock, algorithm
):
    input_image = Mock(name="input image")
    psf_image = Mock(name="psf image")
    mock_data_array.return_value = mock_data_array

    get_item_mock = Mock(name="get_item_mock")
    get_item_mock.return_value = get_item_mock
    input_image.__getitem__ = get_item_mock
    psf_image.__getitem__ = get_item_mock

    deconvolution_params = {
        "fractional_threshold": 0.01,
        "gain": 0.1,
        "niter": 5,
        "scales": [0, 3, 10, 30],
        "threshold": 0.0,
    }

    with pytest.raises(NotImplementedError):
        deconvolution_params["algorithm"] = algorithm
        deconvolve_cube(input_image, psf_image, **deconvolution_params)


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.np.allclose",
    return_value=True,
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.np.ones",
    return_value="np.ones",
)
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.xr.DataArray")
def test_should_throw_exceptions_for_non_suported_algorithms(
    mock_data_array, np_ones_mock, np_all_close_mock
):
    input_image = Mock(name="input image")
    psf_image = Mock(name="psf image")
    mock_data_array.return_value = mock_data_array

    get_item_mock = Mock(name="get_item_mock")
    get_item_mock.return_value = get_item_mock
    input_image.__getitem__ = get_item_mock
    psf_image.__getitem__ = get_item_mock

    deconvolution_params = {
        "algorithm": "non-suported-algorithm",
        "fractional_threshold": 0.01,
        "gain": 0.1,
        "niter": 5,
        "scales": [0, 3, 10, 30],
        "threshold": 0.0,
    }

    with pytest.raises(ValueError):
        deconvolve_cube(input_image, psf_image, **deconvolution_params)


@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.Image.constructor")
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.convolve_fft")
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.xr.apply_ufunc")
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.Gaussian2DKernel")
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution"
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


@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.Image.constructor")
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.convolve_fft")
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.xr.apply_ufunc")
@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.Gaussian2DKernel")
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution"
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
