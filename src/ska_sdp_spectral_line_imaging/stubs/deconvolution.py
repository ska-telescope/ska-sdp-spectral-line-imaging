import logging
from typing import Tuple

import numpy as np
import xarray as xr
from astropy.convolution import Gaussian2DKernel, convolve_fft
from ska_sdp_datamodels.image import Image
from ska_sdp_func_python.image.cleaners import hogbom
from ska_sdp_func_python.image.deconvolution import common_arguments
from ska_sdp_func_python.image.operations import convert_clean_beam_to_pixels


# TODO : Write tests
def deconvolve_cube(
    dirty: Image, psf: Image, sensitivity: Image = None, prefix="", **kwargs
) -> Tuple[Image, Image]:
    """Clean using a variety of algorithms.

    The algorithms available are:

     hogbom: Hogbom CLEAN See: Hogbom CLEAN A&A Suppl, 15, 417, (1974)

     hogbom-complex: Complex Hogbom CLEAN of stokesIQUV image

     msclean: MultiScale CLEAN See: Cornwell, T.J.,
     Multiscale CLEAN (IEEE Journal of Selected Topics in Sig Proc,
     2008 vol. 2 pp. 793-801)

     mfsmsclean, msmfsclean, mmclean: MultiScale Multi-Frequency
     See: U. Rau and T. J. Cornwell,
     “A multi-scale multi-frequency deconvolution algorithm
     for synthesis imaging in radio interferometry,” A&A 532, A71 (2011).

    For example::

        comp, residual = deconvolve_cube(dirty, psf, niter=1000,
                           gain=0.7, algorithm='msclean',
                           scales=[0, 3, 10, 30], threshold=0.01)

    For the MFS clean, the psf must have number of channels >= 2 * nmoment.

    :param dirty: Image dirty image
    :param psf: Image Point Spread Function
    :param sensitivity: Sensitivity image (i.e. inverse noise level)
    :param prefix: Informational message for logging
    :param window_shape: Window image (Bool) - clean where True
    :param mask: Window in the form of an image, overrides window_shape
    :param algorithm: Cleaning algorithm:
                'msclean'|'hogbom'|'hogbom-complex'|'mfsmsclean'
    :param gain: loop gain (float) 0.7
    :param threshold: Clean threshold (0.0)
    :param fractional_threshold: Fractional threshold (0.01)
    :param scales: Scales (in pixels) for multiscale ([0, 3, 10, 30])
    :param nmoment: Number of frequency moments (default 3)
    :param findpeak: Method of finding peak in mfsclean:
                    'Algorithm1'|'ASKAPSoft'|'CASA'|'RASCIL',
                    Default is RASCIL.
    :return: component image, residual image

     See also
        :py:func:`ska_sdp_func_python.image.cleaners.hogbom`
        :py:func:`ska_sdp_func_python.image.cleaners.hogbom_complex`
        :py:func:`ska_sdp_func_python.image.cleaners.msclean`
        :py:func:`ska_sdp_func_python.image.cleaners.msmfsclean`

    """
    logger = logging.getLogger()

    # TODO: port later once window_shape is not None
    # window_shape = kwargs.get("window_shape", None)
    # window = find_window_list(
    #     dirty_list, prefix, window_shape=window_shape
    # )

    window = xr.DataArray(
        np.ones(
            dirty["pixels"].shape,
            dtype=dirty["pixels"].dtype,
        ),
        dims=dirty["pixels"].dims,
        coords=dirty["pixels"].coords,
    ).chunk(dirty["pixels"].chunksizes)

    # check_psf_peak
    pmax = psf["pixels"].max(dim=["polarisation", "y", "x"])

    assert np.allclose(pmax, 1.0), "PSF does not have unit peak"

    # TODO: Take "scales" parameter when required later
    fracthresh, gain, niter, thresh, _ = common_arguments(**kwargs)

    # TODO: port later once psf_support is not None
    # psf_support = kwargs.get("psf_support", None)
    # psf_list = bound_psf_list(
    #     dirty_list, prefix, psf_list, psf_support=psf_support
    # )
    # assert np.allclose(pmax, 1.0) ,"PSF does not have unit peak"

    algorithm = kwargs.get("algorithm", "hogbom")

    # TODO: Port other algorithms
    if algorithm == "msclean":
        raise NotImplementedError("msclean is not implemented")
    elif algorithm in ("msmfsclean", "mfsmsclean", "mmclean"):
        raise NotImplementedError(
            'msmfsclean", "mfsmsclean", "mmclean" are not implemented'
        )

    elif algorithm == "hogbom":
        # TODO: pass "prefix" to hogbom kwargs
        comp, res = xr.apply_ufunc(
            hogbom,
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
            kwargs=dict(
                gain=gain, thresh=thresh, niter=niter, fracthresh=fracthresh
            ),
        )

    elif algorithm == "hogbom-complex":
        raise NotImplementedError("hogbom complex is not complicated")
    else:
        raise ValueError(
            f"deconvolve_cube {prefix}: Unknown algorithm {algorithm}"
        )

    logger.info(f"Deconvolve_cube {prefix}: Deconvolution finished")

    comp_image = Image.constructor(
        data=comp.data,
        polarisation_frame=dirty.image_acc.polarisation_frame,
        wcs=dirty.image_acc.wcs,
    )

    residual_image = Image.constructor(
        data=res.data,
        polarisation_frame=dirty.image_acc.polarisation_frame,
        wcs=dirty.image_acc.wcs,
    )

    return comp_image, residual_image


# TODO : Write tests
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
