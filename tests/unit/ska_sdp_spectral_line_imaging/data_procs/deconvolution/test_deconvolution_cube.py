import pytest
from mock import Mock, patch
from ska_sdp_func_python.image.cleaners import hogbom, msclean

from ska_sdp_spectral_line_imaging.data_procs.deconvolution import deconvolver


@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.deconvolver."
    "common_arguments"
)
@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.deconvolver."
    "Image.constructor"
)
@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.deconvolver."
    "clean_with"
)
def test_should_deconvolve_cube_using_hogbom(
    clean_with_mock,
    image_con_mock,
    common_arguments_mock,
):
    input_image = Mock(name="input image")
    psf_image = Mock(name="psf image")
    component = Mock(name="component")
    residual = Mock(name="residual")
    clean_with_mock.return_value = (component, residual)
    image_con_mock.side_effect = ["component_image", "residual_image"]

    common_arguments_mock.return_value = (
        "frac_thresh",
        "gain",
        "niter",
        "thresh",
        "scales",
    )

    deconvolution_params = {
        "algorithm": "hogbom",
        "fractional_threshold": "frac_thresh",
        "gain": "gain",
        "niter": "niter",
        "scales": "scales",
        "threshold": "thresh",
    }

    component_image, residual_image = deconvolver.deconvolve_cube(
        input_image, psf_image, **deconvolution_params
    )

    clean_with_mock.assert_called_once_with(
        hogbom,
        input_image,
        psf_image,
        None,
        include_sensitivity=False,
        gain="gain",
        thresh="thresh",
        niter="niter",
        fracthresh="frac_thresh",
    )

    common_arguments_mock.assert_called_once_with(
        algorithm="hogbom",
        fractional_threshold="frac_thresh",
        gain="gain",
        niter="niter",
        scales="scales",
        threshold="thresh",
    )

    assert component_image == "component_image"
    assert residual_image == "residual_image"


@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.deconvolver."
    "Image.constructor"
)
@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.deconvolver."
    "clean_with"
)
def test_should_deconvolve_cube_using_msclean_with_default_common_arguments(
    clean_with_mock,
    image_con_mock,
):
    input_image = Mock(name="input image")
    psf_image = Mock(name="psf image")
    component = Mock(name="component")
    residual = Mock(name="residual")
    clean_with_mock.return_value = (component, residual)
    image_con_mock.side_effect = ["component_image", "residual_image"]

    deconvolution_params = {
        "algorithm": "msclean",
    }

    component_image, residual_image = deconvolver.deconvolve_cube(
        input_image, psf_image, **deconvolution_params
    )

    clean_with_mock.assert_called_once_with(
        msclean,
        input_image,
        psf_image,
        None,
        None,
        # Default parameters from common_arguments
        gain=0.1,
        thresh=0.0,
        niter=100,
        fracthresh=0.01,
        scales=[0, 3, 10, 30],
    )

    assert component_image == "component_image"
    assert residual_image == "residual_image"


def test_should_throw_exceptions_for_non_suported_algorithms():
    input_image = Mock(name="input image")
    psf_image = Mock(name="psf image")

    deconvolution_params = {
        "algorithm": "non-suported-algorithm",
    }

    with pytest.raises(ValueError):
        deconvolver.deconvolve_cube(
            input_image, psf_image, **deconvolution_params
        )
