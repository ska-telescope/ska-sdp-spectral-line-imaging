from typing import Tuple

import numpy as np
import xarray as xr
from ska_sdp_datamodels.image import Image

RADLER_AVAILABLE = True
try:
    import radler as rd
except ModuleNotFoundError:  # pragma: no cover
    RADLER_AVAILABLE = False  # pragma: no cover


def radler_deconvolve_channel(
    dirty_channel,
    psf,
    nx=None,
    ny=None,
    cell_size=None,
    algorithm="multiscale",
    niter=500,
    threshold=0.001,
    gain=0.7,
    scales=None,
    **kwargs,
):
    """
    Perform deconvolution using radler for a given channel data and returns
    restored model and the residual data
    Parameters
    ----------
        dirty_channel: numpy.ndarray
            Dirty channel data
        psf: numpy.ndarray
            PSF for the given channel
        nx: int
            Number of x pixels
        ny: int
            Number of y pixels
        cell_size: float
            Cell size of each pixel in the image
        algorithm: str
            Cleaning algorithm: 'multiscale'|'iuwt'|'more_sane'|'generic_clean'
        niter: int
            Maximum number of iterations
        threshold: float
            Clean threshold (0.0)
        gain: float
            loop gain (float) 0.7
        scales: List[int]
            Scales (in pixels) for multiscale ([0, 3, 10, 30])

    Returns
    -------
        Tuple[numpy.ndarray, numpy.ndarray]
    """

    ms_scales = [] if scales is None else scales

    settings = rd.Settings()
    settings.trimmed_image_width = nx
    settings.trimmed_image_height = ny
    settings.pixel_scale.x = cell_size
    settings.pixel_scale.y = cell_size
    settings.minor_iteration_count = niter
    settings.threshold = threshold
    settings.minor_loop_gain = gain

    try:
        settings.algorithm_type = getattr(rd.AlgorithmType, algorithm)
    except AttributeError:
        raise ValueError(
            f"imaging_deconvolve with radler: Unknown algorithm {algorithm}"
        )

    if algorithm == "multiscale" and len(ms_scales) > 0:
        settings.multiscale.scale_list = ms_scales

    restored_radler = np.zeros(dirty_channel.shape, dtype=np.float32)
    rw_dirty_channel = np.copy(dirty_channel)
    rw_psf = np.copy(psf)

    radler_object = rd.Radler(
        settings,
        rw_psf,
        rw_dirty_channel,
        restored_radler,
        0.0,
        rd.Polarization.stokes_i,
    )
    reached_threshold = False
    reached_threshold = radler_object.perform(reached_threshold, 0)

    return restored_radler, rw_dirty_channel


def radler_deconvolve_cube(
    dirty: Image, psf: Image, **kwargs
) -> Tuple[Image, Image]:
    """
    Note: This documentation copied from
    ska_sdp_func_python.image.deconvolution.radler_deconvolve_list

    Clean using the Radler module, using various algorithms.

    The algorithms available are
    (see: https://radler.readthedocs.io/en/latest/tree/cpp/algorithms.html):

     msclean
     iuwt
     more_sane
     generic_clean

    For example::

         comp = radler_deconvolve_cube(dirty_list, psf_list, niter=1000,
                        gain=0.7, algorithm='msclean',
                        scales=[0, 3, 10, 30], threshold=0.01)
    Parameters
    ----------
        dirty: Image
            Cube dirty image
        psf: Image
            Point spread function image cube
        **kwargs: keyword arguments
            Additional keyword arguments

    Returns
    -------
        Component image cube

    """

    if not RADLER_AVAILABLE:
        raise ImportError("Unnable to import radler")

    restored_radler_cube, dirty_cube = xr.apply_ufunc(
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
        kwargs=kwargs,
    )

    comp_image = Image.constructor(
        data=restored_radler_cube.data,
        polarisation_frame=dirty.image_acc.polarisation_frame,
        wcs=dirty.image_acc.wcs,
    )

    residual_image = Image.constructor(
        data=dirty_cube.data,
        polarisation_frame=dirty.image_acc.polarisation_frame,
        wcs=dirty.image_acc.wcs,
    )

    return comp_image, residual_image
