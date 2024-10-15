import logging
from typing import Tuple

import dask
import numpy as np
import xarray as xr
from ska_sdp_datamodels.image import Image
from ska_sdp_func_python.image.cleaners import hogbom, msclean
from ska_sdp_func_python.image.deconvolution import common_arguments

from .cleaners import clean_with


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
        dask.array.ones_like(dirty["pixels"]),
        dims=dirty["pixels"].dims,
        coords=dirty["pixels"].coords,
    ).chunk(dirty["pixels"].chunksizes)

    # check_psf_peak
    pmax = psf["pixels"].max(dim=["polarisation", "y", "x"])

    assert np.allclose(pmax, 1.0), "PSF does not have unit peak"

    # TODO: Take "scales" parameter when required later
    fracthresh, gain, niter, thresh, scales = common_arguments(**kwargs)

    # TODO: port later once psf_support is not None
    # psf_support = kwargs.get("psf_support", None)
    # psf_list = bound_psf_list(
    #     dirty_list, prefix, psf_list, psf_support=psf_support
    # )
    # assert np.allclose(pmax, 1.0) ,"PSF does not have unit peak"

    algorithm = kwargs.get("algorithm", "hogbom")

    # TODO: Port other algorithms
    if algorithm == "msclean":
        comp, res = clean_with(
            msclean,
            dirty,
            psf,
            window,
            sensitivity,
            gain=gain,
            thresh=thresh,
            niter=niter,
            fracthresh=fracthresh,
            scales=scales,
            prefix=prefix,
        )

    elif algorithm == "hogbom":
        comp, res = clean_with(
            hogbom,
            dirty,
            psf,
            window,
            include_sensitivity=False,
            gain=gain,
            thresh=thresh,
            niter=niter,
            fracthresh=fracthresh,
            prefix=prefix,
        )

    else:
        raise ValueError(
            f"deconvolve_cube {prefix}: Unsupported algorithm {algorithm}"
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
