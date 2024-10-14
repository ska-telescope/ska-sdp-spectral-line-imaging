import logging

import numpy as np
import xarray as xr
from astropy.convolution import Gaussian2DKernel, convolve_fft
from ska_sdp_datamodels.image import Image
from ska_sdp_func_python.image.operations import convert_clean_beam_to_pixels


def restore_cube(
    model: Image, clean_beam: dict, residual: Image = None
) -> Image:
    """Restore the model image to the residuals.

    The clean beam can be specified as a dictionary with
    fields "bmaj", "bmin" (both in arcsec) and "bpa" in degrees.

    :param model: Model image (i.e. deconvolved)
    :param psf: Input PSF
    :param residual: Residual Image
    :param clean_beam: Clean beam e.g. {"bmaj":0.1, "bmin":0.05, "bpa":-60.0}.
                        Units are deg, deg, deg
    :return: restored image

    """
    logger = logging.getLogger()

    logger.info(f"clean beam: {clean_beam}")

    # TODO: make dask compatible
    beam_pixels = convert_clean_beam_to_pixels(model, clean_beam)

    gk = Gaussian2DKernel(
        x_stddev=beam_pixels[0],
        y_stddev=beam_pixels[1],
        theta=beam_pixels[2],
    )
    # After the bug fix of astropy>=5.22, needs to normalize 'peak'
    gk.normalize(mode="peak")

    restored = xr.apply_ufunc(
        convolve_fft,
        model["pixels"],
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
            kernel=gk,
            normalize_kernel=False,
            allow_huge=True,
            boundary="wrap",
        ),
    )

    if residual is not None:
        restored += residual["pixels"]

    restored_image = Image.constructor(
        data=restored.data,
        polarisation_frame=model.image_acc.polarisation_frame,
        wcs=model.image_acc.wcs,
    )

    restored_image.attrs["clean_beam"] = clean_beam

    logger.info("Image restoration finished")

    return restored_image
