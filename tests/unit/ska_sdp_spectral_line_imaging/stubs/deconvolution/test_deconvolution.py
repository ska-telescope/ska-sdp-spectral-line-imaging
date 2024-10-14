from mock import patch

from ska_sdp_spectral_line_imaging.stubs.deconvolution import deconvolve


@patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.deconvolve_cube")
def test_should_run_deconvolve_cube_when_use_radler_is_false(
    deconvolve_cube_mock,
):
    deconvolve("dirty", "psf", use_radler=False, kwarg="kwargs")
    deconvolve_cube_mock.assert_called_once_with(
        "dirty", "psf", kwarg="kwargs"
    )


@patch(
    "ska_sdp_spectral_line_imaging.stubs.deconvolution."
    "radler_deconvolve_cube"
)
def test_should_run_radler_deconvolve_cube_when_use_radler_is_false(
    radler_deconvolve_cube_mock,
):
    deconvolve("dirty", "psf", use_radler=True, kwarg="kwargs")
    radler_deconvolve_cube_mock.assert_called_once_with(
        "dirty", "psf", kwarg="kwargs"
    )
