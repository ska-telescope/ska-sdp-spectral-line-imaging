from typing import Tuple

from ska_sdp_datamodels.image import Image

from .deconvolver import deconvolve_cube
from .radler import radler_deconvolve_cube
from .restore_image import restore_cube


def deconvolve(
    dirty: Image, psf: Image, use_radler=False, **kwargs
) -> Tuple[Image, Image]:
    """Clean using a variety algorithms.

    Parameters
    ----------
        dirty (Image): Dirty Image
        psf (Image): Point Spread Function
        use_radler (bool): Use radler to perform deconvolution
            instead of SDP functions
        **kwargs: Keyword arguments

    Returns
    -------
        Tuple[Image, Image]
    """
    if use_radler:
        return radler_deconvolve_cube(dirty, psf, **kwargs)

    return deconvolve_cube(dirty, psf, **kwargs)


__all__ = ["deconvolve", "restore_cube"]
