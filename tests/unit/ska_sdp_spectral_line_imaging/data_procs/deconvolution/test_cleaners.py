import numpy as np
from mock import Mock, patch

from ska_sdp_spectral_line_imaging.data_procs.deconvolution.cleaners import (
    apply_cleaner_with_sensitivity_only,
    clean_with,
)


@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.cleaners."
    "xr.apply_ufunc"
)
def test_should_apply_cleaners_when_window_and_sensitivity_are_none(
    apply_ufunc_mock,
):
    cleaner = Mock(name="Cleaner function")

    dirty = {"pixels": "dirty_pixels"}

    psf = {"pixels": "psf_pixels"}

    clean_with(cleaner, dirty, psf, window=None, kwarg="kwarg")

    apply_ufunc_mock.assert_called_once_with(
        cleaner,
        "dirty_pixels",
        "psf_pixels",
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
        kwargs={"kwarg": "kwarg", "window": None, "sensitivity": None},
    )


@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.cleaners."
    "xr.apply_ufunc"
)
def test_should_apply_cleaners_when_only_sensitivity_is_none(
    apply_ufunc_mock,
):
    cleaner = Mock(name="Cleaner function")

    dirty = {"pixels": "dirty_pixels"}

    psf = {"pixels": "psf_pixels"}

    clean_with(cleaner, dirty, psf, window="window", kwarg="kwarg")

    apply_ufunc_mock.assert_called_once_with(
        cleaner,
        "dirty_pixels",
        "psf_pixels",
        "window",
        input_core_dims=[
            ["y", "x"],
            ["y", "x"],
            ["y", "x"],
        ],
        output_core_dims=[["y", "x"], ["y", "x"]],
        # TODO: parameterize dtype
        output_dtypes=(np.float32, np.float32),
        vectorize=True,
        dask="parallelized",
        keep_attrs=True,
        kwargs={"kwarg": "kwarg", "sensitivity": None},
    )


@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.cleaners."
    "xr.apply_ufunc"
)
def test_should_apply_cleaners_when_wndw_and_senstvty_are_none_without_include(
    apply_ufunc_mock,
):
    cleaner = Mock(name="Cleaner function")

    dirty = {"pixels": "dirty_pixels"}

    psf = {"pixels": "psf_pixels"}

    clean_with(
        cleaner,
        dirty,
        psf,
        window=None,
        include_sensitivity=False,
        kwarg="kwarg",
    )

    apply_ufunc_mock.assert_called_once_with(
        cleaner,
        "dirty_pixels",
        "psf_pixels",
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
        kwargs={"kwarg": "kwarg", "window": None},
    )


@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.cleaners."
    "xr.apply_ufunc"
)
def test_should_apply_cleaners_when_only_sensitivity_is_none_without_includes(
    apply_ufunc_mock,
):
    cleaner = Mock(name="Cleaner function")

    dirty = {"pixels": "dirty_pixels"}

    psf = {"pixels": "psf_pixels"}

    clean_with(
        cleaner,
        dirty,
        psf,
        window="window",
        include_sensitivity=False,
        kwarg="kwarg",
    )

    apply_ufunc_mock.assert_called_once_with(
        cleaner,
        "dirty_pixels",
        "psf_pixels",
        "window",
        input_core_dims=[
            ["y", "x"],
            ["y", "x"],
            ["y", "x"],
        ],
        output_core_dims=[["y", "x"], ["y", "x"]],
        # TODO: parameterize dtype
        output_dtypes=(np.float32, np.float32),
        vectorize=True,
        dask="parallelized",
        keep_attrs=True,
        kwargs={"kwarg": "kwarg"},
    )


@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.cleaners."
    "xr.apply_ufunc"
)
def test_should_apply_cleaners_when_only_window_is_none(
    apply_ufunc_mock,
):
    cleaner = Mock(name="Cleaner function")
    dirty = {"pixels": "dirty_pixels"}

    psf = {"pixels": "psf_pixels"}
    sensitivity = {"pixels": "sensitivity_pixels"}

    clean_with(
        cleaner,
        dirty,
        psf,
        window=None,
        sensitivity=sensitivity,
        kwarg="kwarg",
    )

    apply_ufunc_mock.assert_called_once_with(
        apply_cleaner_with_sensitivity_only,
        "dirty_pixels",
        "psf_pixels",
        "sensitivity_pixels",
        input_core_dims=[
            ["y", "x"],
            ["y", "x"],
            ["y", "x"],
        ],
        output_core_dims=[["y", "x"], ["y", "x"]],
        # TODO: parameterize dtype
        output_dtypes=(np.float32, np.float32),
        vectorize=True,
        dask="parallelized",
        keep_attrs=True,
        kwargs={"kwarg": "kwarg", "cleaner": cleaner},
    )


@patch(
    "ska_sdp_spectral_line_imaging.data_procs.deconvolution.cleaners."
    "xr.apply_ufunc"
)
def test_should_apply_cleaners_when_window_and_sensitivity_are_not_none(
    apply_ufunc_mock,
):
    cleaner = Mock(name="Cleaner function")

    dirty = {"pixels": "dirty_pixels"}

    psf = {"pixels": "psf_pixels"}
    sensitivity = {"pixels": "sensitivity_pixels"}

    clean_with(
        cleaner,
        dirty,
        psf,
        window="window",
        sensitivity=sensitivity,
        kwarg="kwarg",
    )

    apply_ufunc_mock.assert_called_once_with(
        cleaner,
        "dirty_pixels",
        "psf_pixels",
        "window",
        "sensitivity_pixels",
        input_core_dims=[
            ["y", "x"],
            ["y", "x"],
            ["y", "x"],
            ["y", "x"],
        ],
        output_core_dims=[["y", "x"], ["y", "x"]],
        # TODO: parameterize dtype
        output_dtypes=(np.float32, np.float32),
        vectorize=True,
        dask="parallelized",
        keep_attrs=True,
        kwargs={"kwarg": "kwarg"},
    )


def test_apply_cleaner_with_senstvty_only_should_call_appropriate_function():
    cleaner = Mock(name="Cleaner function")
    dirty = {"pixels": "dirty_pixels"}

    psf = {"pixels": "psf_pixels"}
    sensitivity = {"pixels": "sensitivity_pixels"}

    apply_cleaner_with_sensitivity_only(
        dirty, psf, sensitivity, cleaner, kwarg="kwarg"
    )

    cleaner.assert_called_once_with(
        dirty, psf, None, sensitivity, kwarg="kwarg"
    )
