import pytest
from mock import Mock, patch
from ska_sdp_func_python.image.cleaners import hogbom, msclean

from ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver import (
    deconvolve_cube,
)


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "Image.constructor"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "dask.array.ones_like",
    return_value="np.ones",
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "np.allclose",
    return_value=True,
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "clean_with"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "xr.DataArray"
)
def test_should_deconvolve_cube_using_hogbom(
    mock_data_array,
    clean_with_mock,
    np_all_close_mock,
    np_ones_mock,
    image_con_mock,
):
    input_image = Mock(name="input image")
    psf_image = Mock(name="psf image")
    component = Mock(name="component")
    residual = Mock(name="residual")
    mock_data_array.return_value = mock_data_array
    mock_data_array.chunk.return_value = "WINDOW"
    clean_with_mock.return_value = (component, residual)
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

    clean_with_mock.assert_called_once_with(
        hogbom,
        input_image,
        psf_image,
        "WINDOW",
        include_sensitivity=False,
        gain=0.1,
        thresh=0.0,
        niter=5,
        fracthresh=0.01,
        prefix="",
    )

    assert component_image == "component_image"
    assert residual_image == "residual_image"

    mock_data_array.assert_called_once_with(
        "np.ones",
        dims=input_image["pixels"].dims,
        coords=input_image["pixels"].coords,
    )


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "Image.constructor"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "dask.array.ones_like",
    return_value="np.ones",
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "np.allclose",
    return_value=True,
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "clean_with"
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "xr.DataArray"
)
def test_should_deconvolve_cube_using_msclean(
    mock_data_array,
    clean_with_mock,
    np_all_close_mock,
    np_ones_mock,
    image_con_mock,
):
    input_image = Mock(name="input image")
    psf_image = Mock(name="psf image")
    component = Mock(name="component")
    residual = Mock(name="residual")
    mock_data_array.return_value = mock_data_array
    mock_data_array.chunk.return_value = "WINDOW"
    clean_with_mock.return_value = (component, residual)
    image_con_mock.side_effect = ["component_image", "residual_image"]

    get_item_mock = Mock(name="get_item_mock")
    get_item_mock.return_value = get_item_mock
    input_image.__getitem__ = get_item_mock
    psf_image.__getitem__ = get_item_mock

    deconvolution_params = {
        "algorithm": "msclean",
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

    clean_with_mock.assert_called_once_with(
        msclean,
        input_image,
        psf_image,
        "WINDOW",
        None,
        gain=0.1,
        thresh=0.0,
        niter=5,
        fracthresh=0.01,
        scales=[0, 3, 10, 30],
        prefix="",
    )

    assert component_image == "component_image"
    assert residual_image == "residual_image"

    mock_data_array.assert_called_once_with(
        "np.ones",
        dims=input_image["pixels"].dims,
        coords=input_image["pixels"].coords,
    )


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "np.allclose",
    return_value=True,
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "dask.array.ones_like",
    return_value="np.ones",
)
@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolver."
    "xr.DataArray"
)
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
