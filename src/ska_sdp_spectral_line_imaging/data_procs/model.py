import logging
from typing import Tuple

import dask
import dask.array
import dask.delayed
import numpy as np
import xarray as xr
from astropy.io import fits
from astropy.wcs import WCS

logger = logging.getLogger()

fits_codes_to_pol_names = {
    1: "I",
    2: "Q",
    3: "U",
    4: "V",
    -1: "RR",
    -2: "LL",
    -3: "RL",
    -4: "LR",
    -5: "XX",
    -6: "YY",
    -7: "XY",
    -8: "YX",
}

fits_axis_to_image_dims = {
    "RA": "x",
    "DEC": "y",
    "FREQ": "frequency",
    "STOKES": "polarization",
}


@dask.delayed
def read_fits_memmapped_delayed(image_path, hduid=0):
    with fits.open(
        image_path, mode="denywrite", memmap=True, lazy_load_hdus=True
    ) as hdul:
        hdu = hdul[hduid]
        data = hdu.data

    return data


@dask.delayed
def report_peak_visibility(visibility, unit):
    abs_visibility = np.abs(visibility)
    max_freq_axis = abs_visibility.max(
        dim=["time", "baseline_id", "polarization"]
    )
    peak_channel = max_freq_axis.argmax()
    peak_frequency = max_freq_axis.idxmax()
    max_visibility = abs_visibility.max()

    logger.info(
        "Peak visibility Channel: {peak_channel}."
        " Frequency: {peak_frequency} {unit}."
        " Peak Visibility: {max_visibility}".format(
            peak_channel=peak_channel.values,
            peak_frequency=peak_frequency.values,
            max_visibility=max_visibility.values,
            unit=unit,
        )
    )


def get_dask_array_from_fits(
    image_path: str,
    hduid: int,
    shape: Tuple,
    dtype: type,
):
    data = dask.array.from_delayed(
        read_fits_memmapped_delayed(image_path, hduid),
        shape=shape,
        dtype=dtype,
    )

    return data


def get_dataarray_from_fits(image_path, hduid=0):
    """
    Reads FITS image and returns an xarray dataarray with
    dimensions ["polarization", "frequency", "y", "x"] or
    only ["y", "x"] if data is 2 dimensionsional.

    Function can also read coordinte values for dimensions "polarization"
    and "frequency". Spatial coordinates "y" and "x" are linear, and
    the their coordinate values are not populatedin output dataarray.
    If needed, those can be populated later.
    Refer ska_sdp_datamodels.image.Image.constructor.

    The image data is read as a dask array using delayed read calls to
    astropy.fits.open.

    Parameters
    ----------
    image_path: str
        Path to FITS image

    hduid: int
        The HDU number in the HDUList read from FITS image.

    Returns
    -------
        xarray.DataArray

    Raises
    ------
        NotImplementedError
            If chunksizes are passed as parameter
    """
    # opening image only to get metadata
    with fits.open(image_path, memmap=True) as hdul:
        hdu = hdul[hduid]
        shape = hdu.data.shape
        dtype = hdu.data.dtype

    wcs = WCS(image_path)

    dimensions = [
        fits_axis_to_image_dims[axis] for axis in reversed(wcs.axis_type_names)
    ]

    coordinates = {}
    if "frequency" in dimensions:
        spectral_wcs = wcs.sub(["spectral"])
        frequency_range = spectral_wcs.wcs_pix2world(
            range(spectral_wcs.pixel_shape[0]), 0
        )[0]
        coordinates["frequency"] = frequency_range
    if "polarization" in dimensions:
        pol_wcs = wcs.sub(["stokes"])
        pol_codes = pol_wcs.wcs_pix2world(range(pol_wcs.pixel_shape[0]), 0)[0]
        pol_names = [fits_codes_to_pol_names[code] for code in pol_codes]
        coordinates["polarization"] = pol_names

    data = get_dask_array_from_fits(image_path, hduid, shape, dtype)

    return xr.DataArray(
        data,
        dims=dimensions,
        coords=coordinates,
        name="fits_image_arr",
    )


def apply_power_law_scaling(
    image: xr.DataArray,
    frequency_range: np.ndarray,
    reference_frequency: float = None,
    spectral_index: float = 0.75,
):
    """
    Apply power law scaling on a continuum image and return a scaled cube.

    The "image" must be a xarray dataarray.
    If "frequency" dimension of size 1 is present in image,
    then that value is used as reference frequency for scaling.
    Else, user can pass any reference frequency
    as "reference_frequency" argument, which takes preference.

    If "data" attribute of image is a dask array, then this will return
    a dataarray which wraps a new dask array containing operations
    for power law scaling. This means, the values in returned dataarray
    are not computed eagerly.

    The formula for power law scaling is given as:

    ..math

        S2 = S1 * ( \\nu2 / \\nu1 ) ^ {-\\alpha}

    Parameters
    ----------
        image: xr.DataArray
            Image data array. The "data" attribute of dataarray can either
            be a numpy array or a dask array.
        frequency_range: numpy.ndarray | dask.array
            Frequency range in hertz over which to scale the data.
        reference_frequency: float, optional
            Refernce frequency in hertz. If not passed, function expects that
            a frequency coordinate is present in the image. If passed, this
            takes priority over the frequency cordinates of image.
        spectral_index: float, optional
            Spectral index (alpha) used in power law scaling.
            Defaults to 0.75.

    Returns
    -------
        xr.DataArray
            A 3-dimensional scaled spectral cube
    """
    _ref_freq = None
    if "frequency" in image.dims:
        if image.frequency.size != 1:
            logger.warn(
                "Can not apply power law scaling on a spectral cube."
                "Ignoring passed argument and continuing pipeline"
            )
            return image

        _ref_freq = image.frequency.data
        # Need to remove frequency dimension
        # for broadcasting to work later
        image = image.squeeze(dim="frequency", drop=True)

    if reference_frequency:
        _ref_freq = reference_frequency

    if _ref_freq is None:
        raise Exception(
            "reference_frequency is not passed, and "
            "'frequency' dimension is not present in the input image. "
            "Can not proceed with power law scaling."
        )

    channel_multipliers = np.power(
        (frequency_range / _ref_freq), -spectral_index
    )
    channel_multipliers_da = xr.DataArray(
        channel_multipliers,
        dims=["frequency"],
        coords={"frequency": frequency_range},
    )

    scaled_cube = image * channel_multipliers_da
    return scaled_cube


def fit_polynomial_on_visibility(data):
    """
    Perform polynomial fit across frequency axis.

    Parameters
    ----------
    data: xarray.DataArray
        A DataArray with dimensions ["time", "baseline_id", "polarization",
        "frequencies"] in any sequence.

    Returns
    -------
    dask.delayed.Delayed
        A dask delayed call to numpy polynomial polyfit function
    """
    mean_vis = data.mean(
        dim=["time", "baseline_id", "polarization"], skipna=True
    )
    weights = np.isfinite(mean_vis)
    mean_vis_finite = xr.where(weights, mean_vis, 0.0)
    xaxis = dask.array.arange(mean_vis_finite.size)

    return dask.delayed(np.polynomial.polynomial.polyfit)(
        xaxis, mean_vis_finite, w=weights, deg=1
    )
