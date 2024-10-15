import logging

import numpy as np
import xarray as xr

logger = logging.getLogger()


def apply_cleaner_with_sensitivity_only(
    dirty, psf, sensitivity, cleaner, **kwargs
):
    """
    Applies the clean function with sensitivity only

    Parameters
    ----------
        dirty: xr.DataArray
           Image to be cleaned
        psf: xr.DataArray
           PSF Image
        sensitivity: xr.DataArray
           Sensitivity data
        cleaner: func
           cleaner function to be applied
        **kwargs:
           Additional keyword arguments

    Returns
    -------
        Returns the data products from the clean algorithm

    """
    return cleaner(dirty, psf, None, sensitivity, **kwargs)


def clean_with(
    cleaner,
    dirty,
    psf,
    window,
    sensitivity=None,
    include_sensitivity=True,
    **kwargs,
):
    """
    Helps to manage parameters for xr.apply_ufunc

    Parameters
    ----------
        cleaner: func
           cleaner function to be applied
        dirty: Image
           Image to be cleaned
        psf: Image
           PSF Image
        window: xr.DataArray
           Window image (Bool) - clean where True
        sensitivity: Image
           Sensitivity Image
        include_sensitivity: bool
           Should sensitivity be included in kwargs
        **kwargs:
           Additional keyword arguments

    Returns
    -------
        Returns component image and residual image
    """
    if window is None and sensitivity is not None:
        kwargs["cleaner"] = cleaner
        return xr.apply_ufunc(
            apply_cleaner_with_sensitivity_only,
            dirty["pixels"],
            psf["pixels"],
            sensitivity["pixels"],
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
            kwargs=kwargs,
        )

    if window is None and sensitivity is None:
        kwargs["window"] = None
        if include_sensitivity:
            kwargs["sensitivity"] = None

        return xr.apply_ufunc(
            cleaner,
            dirty["pixels"],
            psf["pixels"],
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
            kwargs=kwargs,
        )

    if window is not None and sensitivity is None:
        if include_sensitivity:
            kwargs["sensitivity"] = None

        return xr.apply_ufunc(
            cleaner,
            dirty["pixels"],
            psf["pixels"],
            window,
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
            kwargs=kwargs,
        )

    return xr.apply_ufunc(
        cleaner,
        dirty["pixels"],
        psf["pixels"],
        window,
        sensitivity["pixels"],
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
        kwargs=kwargs,
    )
