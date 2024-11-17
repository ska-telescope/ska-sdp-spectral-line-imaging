from typing import Tuple

import dask
import numpy as np
import xarray as xr
from astropy import units as au
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.wcs import WCS
from ska_sdp_datamodels.science_data_model.polarisation_model import (
    PolarisationFrame,
)

from ska_sdp_spectral_line_imaging.constants import (
    FITS_AXIS_TO_IMAGE_DIM,
    FITS_CODE_TO_POL_NAME,
)


def rechunk(target, ref, dim):
    """
    Rechunk a target DataArray based on a ref DataArray

    Parameters
    ----------
        target: xr.DataArray
            DataArray to be rechunked
        ref: xr.DataArray
            Reference DataArray
        dim: dict
            Dimensions to be expanded along

    Returns
    -------
        xr.DataArray
    """
    return (
        target.expand_dims(dim=dim).transpose(*ref.dims).chunk(ref.chunksizes)
    )


def export_image_as(image, output_path, export_format="fits"):
    """
    Export data in the desired export_format

    Parameters
    ----------
        image: ska_sdp_datamodels.image.image_model.Image
            Image to be exported
        output_path: str
            Output file name
        export_format: str
            Data format for the data. Allowed values: fits|zarr

    Returns
    -------
        dask.delayed.Delayed

    Raises
    ------
        ValueError:
            If the provided data format is not in fits or zarr
    """

    if export_format == "fits":
        return export_to_fits(image, output_path)
    elif export_format == "zarr":
        return export_to_zarr(image, output_path)
    else:
        raise ValueError(f"Unsupported format: {export_format}")


def export_to_zarr(data, output_path, clear_attrs=False):
    """
    Lazily export xarray dataset/dataarray to zarr file format.

    Parameters
    ----------
        data: xarray.DataArray | xarray.Dataset
            Xarray data to be exported
        output_path: str
            Output file path. A ".zarr" is appended to this path.
        clear_attrs: bool = False
            Whether to clear attributes of the data before writing to zarr.

    Returns
    -------
        dask.delayed.Delayed
            A dask delayed object which represents the task of writing
            data to zarr.
    """
    data_to_export = data.copy(deep=False)
    if clear_attrs:
        data_to_export.attrs.clear()
    return data_to_export.to_zarr(store=f"{output_path}.zarr", compute=False)


@dask.delayed
def export_to_fits(image, output_path):
    """
    Lazily export an image to FITS file format.

    Parameters
    ----------
        image: ska_sdp_datamodels.image.image_model.Image
            Image image to be exported
        output_path: str
            Output file path. A ".fits" is appended to this path.

    Returns
    -------
        dask.delayed.Delayed
            A dask delayed object which represents the task of writing
            image to FITS.
    """
    image.image_acc.export_to_fits(f"{output_path}.fits")


def estimate_cell_size_in_arcsec(
    baseline: float, wavelength: float, factor=3.0
) -> float:
    """
    A generalized function which estimates cell size for given baseline value.

    This function is dask compatible i.e. can take dask arrays as input,
    and return dask array as output.

    Parameters
    ----------
        baseline: float
            Baseline length in meters. For better estimation, this has to be
            the maximum baseline length in any direction.
        wavelength: float
            Wavelength in meters. For better estimation, it has to be
            the minimum wavelength observed.
        factor: float
            Scaling factor.

    Returns
    -------
        float
            Cell size in arcsecond.
            **The output is rounded** to the 2 decimal places.
    """
    baseline = baseline / wavelength

    cell_size_rad = 1.0 / (2.0 * factor * baseline)

    cell_size_arcsec = np.rad2deg(cell_size_rad) * 3600

    # Rounded to 2 decimals
    return cell_size_arcsec.round(2)


def estimate_image_size(
    wavelength: float, antenna_diameter: float, cell_size: float
) -> int:
    """
    Estimates dimension of the image which will be used in the imaging stage.

    This function is dask compatible i.e. can take dask arrays as input,
    and return dask array as output.

    Parameters
    ----------
        wavelength: float
            Wavelength in meters. For better estimation,
            this has to be the maximum wavelength observed.
        antenna_diameter: float
            Diameter of the antenna in meters. For better estimation,
            this has to be the minimum of the diameters of all antennas.
        cell_size: float
            Cell size in arcsecond.

    Returns
    -------
        int
            Size of the image.
            **The output is rounded** to the nearest multiple of 100
            greater than the calculated image size.
    """
    cell_size_rad = np.deg2rad(cell_size / 3600)

    image_size = (1.5 * wavelength) / (cell_size_rad * antenna_diameter)

    # Rounding to the nearest multiple of 100
    return np.ceil(image_size / 100) * 100


def get_polarization_frame_from_observation(
    observation: xr.Dataset,
) -> PolarisationFrame:
    """
    Reads an observation from the xradio processing set,
    and generates a PolarizationFrame instance from the
    polarization coordinates.
    This is required to generate an instance of Image class.

    Parameters
    ----------
        observation: xarray.Dataset
            Observation from xradio processing set

    Returns
    -------
        PolarisationFrame
    """
    polarization_lookup = {
        "_".join(value): key
        for key, value in PolarisationFrame.polarisation_frames.items()
    }

    polarization_frame = PolarisationFrame(
        polarization_lookup["_".join(observation.polarization.data)]
    )

    return polarization_frame


# NOTE: This does not handle MOMENT images, only FREQ
def get_wcs_from_observation(observation, cell_size, nx, ny) -> WCS:
    """
    Reads an observation from the xradio processing set,
    and extracts WCS information.
    This is required to create an instance of Image class
    defined in `ska_sdp_datamodels.image.Image`.

    Since Image dimensions are fixed to
    ["frequency", "polarisation", "y", "x"], the sequence of axes in WCS is
    ["RA", "DEC", "STOKES", "FREQ"].

    **NOTE:** Polarization axis is defaulted to stokes, with crval = 1.0 and
    cdelta = 1.0. This is done due to the difference in the sequence of
    linear and circular polarizations values in FITS and in processing set.
    Consumer of this function is expected to populate correct values for
    polarizations present in the processing set.

    Parameters
    ----------
        observation: xarray.Dataset
            Observation from xradio processing set
        cell_size: float
            Cell size in arcseconds.
        nx: int
            Image size X
        ny: int
            Image size Y

    Returns
    -------
        WCS
    """
    field_phase_center = (
        observation.VISIBILITY.field_and_source_xds.FIELD_PHASE_CENTER
    )

    if set(field_phase_center.sky_dir_label.values) != {"ra", "dec"}:
        raise ValueError(
            "Phase field center coordinates labels are not equal to RA / DEC."
        )
    if set(field_phase_center.units) != {"rad"}:
        raise ValueError("Phase field center value is not defined in radian.")

    # computes FIELD_PHASE_CENTER if its a dask array
    fp_center = {
        label: value
        for label, value in zip(
            field_phase_center.sky_dir_label.values,
            field_phase_center.to_numpy(),
        )
    }
    fp_frame = field_phase_center.frame.lower()

    # TODO: Verify: Is the fp_frame equal to frame?
    coord = SkyCoord(
        ra=fp_center["ra"] * au.rad,
        dec=fp_center["dec"] * au.rad,
        frame=fp_frame,
    )

    cell_size_degree = cell_size / 3600
    freq_channel_width = observation.frequency.channel_width["data"]
    ref_freq = observation.frequency.reference_frequency["data"]
    freq_unit = observation.frequency.units[0]

    # NOTE: Hardcoding to stokes polarization,
    # consumer should fill-in correct polarization value.
    ref_pol = 1.0
    del_pol = 1.0

    new_wcs = WCS(naxis=4)

    # computes nx and ny if those are dask arrays
    new_wcs.wcs.crpix = [nx // 2, ny // 2, 1, 1]
    new_wcs.wcs.cunit = ["deg", "deg", "", freq_unit]
    # computes cell_size_degree if its a dask array
    new_wcs.wcs.cdelt = [
        -cell_size_degree,
        cell_size_degree,
        del_pol,
        freq_channel_width,
    ]
    new_wcs.wcs.crval = [coord.ra.deg, coord.dec.deg, ref_pol, ref_freq]
    new_wcs.wcs.ctype = ["RA---SIN", "DEC--SIN", "STOKES", "FREQ"]

    # NOTE: "ICRS" since sdp-datamodels also have fixed radesys
    new_wcs.wcs.radesys = "ICRS"
    # new_wcs.wcs.radesys = coord.frame.name.upper()

    # NOTE: "2000.0" since sdp-datamodels also have fixed equinox
    new_wcs.wcs.equinox = 2000.0
    # new_wcs.wcs.equinox = coord.frame.equinox.jyear

    # NOTE: Verify this assignment is correct
    new_wcs.wcs.specsys = observation.frequency.frame

    return new_wcs


@dask.delayed
def read_fits_memmapped_delayed(image_path, hduid=0):
    with fits.open(
        image_path, mode="denywrite", memmap=True, lazy_load_hdus=True
    ) as hdul:
        hdu = hdul[hduid]
        data = hdu.data

    return data


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
        FITS_AXIS_TO_IMAGE_DIM[axis] for axis in reversed(wcs.axis_type_names)
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
        pol_names = [FITS_CODE_TO_POL_NAME[code] for code in pol_codes]
        coordinates["polarization"] = pol_names

    data = get_dask_array_from_fits(image_path, hduid, shape, dtype)

    return xr.DataArray(
        data,
        dims=dimensions,
        coords=coordinates,
        name="fits_image_arr",
    )
