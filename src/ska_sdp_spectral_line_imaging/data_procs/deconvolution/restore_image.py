import logging
import warnings

import numpy as np
import xarray as xr
from astropy.convolution import Gaussian2DKernel, convolve_fft
from astropy.modeling import fitting, models
from ska_sdp_datamodels.image import Image

logger = logging.getLogger()


def convert_clean_beam_to_pixels(cellsize, clean_beam):
    """Convert clean beam to pixels.

    :param cellsize: cellsize in radians
    :param clean_beam: e.g. {"bmaj":0.1, "bmin":0.05, "bpa":-60.0}.
                Units are deg, deg, deg
    :return: Beam size in pixels
    """
    to_mm = np.sqrt(8.0 * np.log(2.0))
    # Beam in pixels
    beam_pixels = (
        np.deg2rad(clean_beam["bmin"]) / (cellsize * to_mm),
        np.deg2rad(clean_beam["bmaj"]) / (cellsize * to_mm),
        np.deg2rad(clean_beam["bpa"]),
    )
    return np.array(beam_pixels)


def fit_psf(psf: np.ndarray):
    """
    Fit a two-dimensional Gaussian to a PSF using astropy.modeling.

    :params psf: Input 2D PSF, with dimensions nx * ny, where nx = ny
    :return: Beam size in pixels
    """
    npixel = psf.shape[1]
    sl = slice(npixel // 2 - 7, npixel // 2 + 8)
    y, x = np.mgrid[sl, sl]
    z = psf[sl, sl]

    p_init = models.Gaussian2D(
        amplitude=np.max(z), x_mean=np.mean(x), y_mean=np.mean(y)
    )
    fit_p = fitting.LevMarLSQFitter()
    with warnings.catch_warnings():
        # Ignore model linearity warning from the fitter
        warnings.simplefilter("ignore")
        fit = fit_p(p_init, x, y, z)
    if fit.x_stddev <= 0.0 or fit.y_stddev <= 0.0:
        logger.warning(
            "fit_psf: error in fitting to psf, using 1 pixel stddev"
        )
        beam_pixels = (1.0, 1.0, 0.0)
    else:
        # Note that the order here is minor, major, pa
        beam_pixels = (
            fit.x_stddev.value,
            fit.y_stddev.value,
            fit.theta.value,
        )

    return np.array(beam_pixels)


def restore_channel(model, beam_pixels):
    gk = Gaussian2DKernel(
        x_stddev=beam_pixels[0],
        y_stddev=beam_pixels[1],
        theta=beam_pixels[2],
    )
    # After the bug fix of astropy>=5.22, needs to normalize 'peak'
    gk.normalize(mode="peak")

    return convolve_fft(
        model,
        gk,
        normalize_kernel=False,
        allow_huge=True,
        boundary="wrap",
    )


def restore_cube(
    model: Image,
    psf: Image,
    residual: Image = None,
    clean_beam: dict = {"bmaj": None, "bmin": None, "bpa": None},
) -> Image:
    """
    Note: This documentation copied from
    ska_sdp_func_python.image.deconvolution.restore_cube.

    Restore the model image to the residuals.

    The clean beam can be specified as a dictionary with
    fields "bmaj", "bmin" (both in arcsec) and "bpa" in degrees.

    :param model: Model image (i.e. deconvolved)
    :param psf: Input PSF
    :param residual: Residual Image
    :param clean_beam: Clean beam e.g. {"bmaj":0.1, "bmin":0.05, "bpa":-60.0}.
                        Units are deg, deg, deg
    :return: restored image

    """
    set_clean_beam = False
    if any(value is None for value in clean_beam.values()):
        logger.info(
            "Will fit psf to get clean_beam per channel per polarization..."
        )
        beam_pixels = xr.apply_ufunc(
            fit_psf,
            psf["pixels"],
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
        )
    else:
        # cellsize in radians
        cellsize = np.deg2rad(model.image_acc.wcs.wcs.cdelt[1])
        beam_pixels = convert_clean_beam_to_pixels(cellsize, clean_beam)
        beam_pixels = xr.DataArray(beam_pixels, dims=["beam"])
        set_clean_beam = True

    restored = xr.apply_ufunc(
        restore_channel,
        model["pixels"],
        beam_pixels,
        input_core_dims=[
            ["y", "x"],
            ["beam"],
        ],
        output_core_dims=[["y", "x"]],
        # TODO: parameterize dtype
        output_dtypes=(np.float32),
        vectorize=True,
        dask="parallelized",
        keep_attrs=True,
    )

    if residual is not None:
        restored = restored + residual["pixels"]

    restored_image = Image.constructor(
        data=restored.data,
        polarisation_frame=model.image_acc.polarisation_frame,
        wcs=model.image_acc.wcs,
    )

    if set_clean_beam:
        restored_image.attrs["clean_beam"] = clean_beam

    return restored_image
