from typing import Tuple

from ska_sdp_datamodels.image import Image
from ska_sdp_func_python.image.cleaners import hogbom, msclean
from ska_sdp_func_python.image.deconvolution import common_arguments

from .cleaners import clean_with


def deconvolve_cube(dirty: Image, psf: Image, **kwargs) -> Tuple[Image, Image]:
    """
    Note: This documentation copied from
    ska_sdp_func_python.image.deconvolution.deconvolve_cube.
    Not all parameters and algorithms are currently supported.

    Clean using a variety of algorithms.

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
    :param window_shape: Window description
    :param mask: Window in the form of an image, overrides window_shape
    :param algorithm: Cleaning algorithm:
                'msclean'|'hogbom'
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
    # TODO: Support missing parameters
    # Refer to ska-sdp-func-python's deconvolve_cube
    # and implement if required.
    sensitivity = None  # must be an Image instance
    window = None  # must be an Image instance

    fracthresh, gain, niter, thresh, scales = common_arguments(**kwargs)
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
        )

    else:
        raise ValueError(f"Deconvolve_cube: Unsupported algorithm {algorithm}")

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
