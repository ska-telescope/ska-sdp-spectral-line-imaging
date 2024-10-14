import numpy as np
import pytest
from mock import Mock, call, patch

import ska_sdp_spectral_line_imaging.stubs.deconvolution.radler as radler_dec
from ska_sdp_spectral_line_imaging.stubs.deconvolution.radler import (
    RADLER_AVAILABLE,
    radler_deconvolve_channel,
    radler_deconvolve_cube,
)

# pylint: disable=import-error,import-outside-toplevel


def test_should_raise_exception_if_radler_not_installed():
    actual_value = RADLER_AVAILABLE
    radler_dec.RADLER_AVAILABLE = False

    with pytest.raises(ImportError):
        radler_deconvolve_cube(
            "dirty", "psf", nx="nx", ny="ny", cell_size="cell_size"
        )

    radler_dec.RADLER_AVAILABLE = actual_value


@pytest.mark.skipif(
    not RADLER_AVAILABLE, reason="Radler is required for this test"
)
class TestRadler:
    @patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.Image")
    @patch(
        "ska_sdp_spectral_line_imaging.stubs.deconvolution.radler."
        "xr.apply_ufunc"
    )
    @patch(
        "ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.rd.Settings"
    )
    def test_should_do_convolution_using_radler(
        self, settings_mock, apply_u_func_mock, image_mock
    ):

        dirty = Mock(name="dirty")
        psf = Mock(name="psf")
        settings_mock.return_value = settings_mock

        restored_model = Mock(name="restored_image")
        dirty_cube = Mock(name="dirty_cube")

        apply_u_func_mock.return_value = [restored_model, dirty_cube]

        psf = Mock(name="psf")
        nx = "nx"
        ny = "ny"
        cell_size = "cell_size"
        radler_deconvolve_cube(dirty, psf, nx=nx, ny=ny, cell_size=cell_size)

        apply_u_func_mock.assert_called_once_with(
            radler_deconvolve_channel,
            dirty.pixels,
            psf.pixels,
            input_core_dims=[
                ["y", "x"],
                ["y", "x"],
            ],
            output_core_dims=[["y", "x"], ["y", "x"]],
            # TODO: parameterize dtype
            output_dtypes=(np.float32, np.float32),
            vectorize=True,
            dask="parallelized",
            keep_attrs=True,
            kwargs=dict(settings=settings_mock),
        )

        image_mock.constructor.assert_has_calls(
            [
                call(
                    data=restored_model.data,
                    polarisation_frame=dirty.image_acc.polarisation_frame,
                    wcs=dirty.image_acc.wcs,
                ),
                call(
                    data=dirty_cube.data,
                    polarisation_frame=dirty.image_acc.polarisation_frame,
                    wcs=dirty.image_acc.wcs,
                ),
            ]
        )

    @patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.Image")
    @patch(
        "ska_sdp_spectral_line_imaging.stubs.deconvolution.radler."
        "xr.apply_ufunc"
    )
    @patch(
        "ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.rd.Settings"
    )
    def test_should_set_scales_in_setting_for_radler(
        self, settings_mock, apply_u_func_mock, image_mock
    ):

        dirty = Mock(name="dirty")
        psf = Mock(name="psf")
        settings_mock.return_value = settings_mock

        restored_model = Mock(name="restored_image")
        dirty_cube = Mock(name="dirty_cube")

        apply_u_func_mock.return_value = [restored_model, dirty_cube]

        psf = Mock(name="psf")
        nx = "nx"
        ny = "ny"
        cell_size = "cell_size"
        radler_deconvolve_cube(
            dirty, psf, nx=nx, ny=ny, cell_size=cell_size, scales="scales"
        )

        assert settings_mock.multiscale.scale_list == "scales"

    @pytest.mark.parametrize(
        "algorithm",
        [
            "multiscale",
            "iuwt",
            "more_sane",
            "generic_clean",
        ],
    )
    @patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.Image")
    @patch(
        "ska_sdp_spectral_line_imaging.stubs.deconvolution.radler."
        "xr.apply_ufunc"
    )
    @patch(
        "ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.rd.Settings"
    )
    def test_should_do_set_appropriate_algorithm_type_for_radler(
        self,
        settings_mock,
        apply_u_func_mock,
        image_mock,
        algorithm,
    ):
        import radler as rd

        algorithm_map = {
            "multiscale": rd.AlgorithmType.multiscale,
            "iuwt": rd.AlgorithmType.iuwt,
            "more_sane": rd.AlgorithmType.more_sane,
            "generic_clean": rd.AlgorithmType.generic_clean,
        }

        algorithm_type = algorithm_map[algorithm]

        dirty = Mock(name="dirty")
        psf = Mock(name="psf")
        settings_mock.return_value = settings_mock

        restored_model = Mock(name="restored_image")
        dirty_cube = Mock(name="dirty_cube")

        apply_u_func_mock.return_value = [restored_model, dirty_cube]

        psf = Mock(name="psf")
        nx = "nx"
        ny = "ny"
        cell_size = "cell_size"
        radler_deconvolve_cube(
            dirty, psf, nx=nx, ny=ny, cell_size=cell_size, algorithm=algorithm
        )

        assert settings_mock.algorithm_type == algorithm_type

    @patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.Image")
    @patch(
        "ska_sdp_spectral_line_imaging.stubs.deconvolution.radler."
        "xr.apply_ufunc"
    )
    @patch(
        "ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.rd.Settings"
    )
    def test_should_throw_exception_for_unknown_algorithm_type_for_radler(
        self, settings_mock, apply_u_func_mock, image_mock
    ):

        dirty = Mock(name="dirty")
        psf = Mock(name="psf")
        settings_mock.return_value = settings_mock

        restored_model = Mock(name="restored_image")
        dirty_cube = Mock(name="dirty_cube")

        apply_u_func_mock.return_value = [restored_model, dirty_cube]

        psf = Mock(name="psf")
        nx = "nx"
        ny = "ny"
        cell_size = "cell_size"
        with pytest.raises(ValueError):
            radler_deconvolve_cube(
                dirty,
                psf,
                nx=nx,
                ny=ny,
                cell_size=cell_size,
                algorithm="unknown-algorithm",
            )

    @patch(
        "ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.rd.Radler"
    )
    @patch("ska_sdp_spectral_line_imaging.stubs.deconvolution.radler.np")
    def test_should_perform_deconvolution_per_channel_using_radler(
        self, numpy_mock, radler_mock
    ):
        import radler as rd

        dirty = Mock(name="dirty")
        psf = Mock(name="psf")
        dirty_copy = Mock(name="dirty_copy")
        psf_copy = Mock(name="psf_copy")
        restored_radler = Mock(name="restored_radler")
        settings_mock = Mock(name="settings")
        numpy_mock.zeros.return_value = restored_radler
        numpy_mock.copy.side_effect = [dirty_copy, psf_copy]

        radler_mock.return_value = radler_mock

        model, residual = radler_deconvolve_channel(dirty, psf, settings_mock)

        radler_mock.assert_called_once_with(
            settings_mock,
            psf_copy,
            dirty_copy,
            restored_radler,
            0.0,
            rd.Polarization.stokes_i,
        )

        radler_mock.perform.assert_called_once_with(False, 0)

        assert model == restored_radler
        assert residual == dirty_copy
