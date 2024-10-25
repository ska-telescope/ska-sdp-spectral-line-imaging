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


def export_data_as(data, output_path, export_format="fits"):
    """
    Export data in the desired export_format

    Parameters
    ----------
        data: Xarray Data array
            Data to be exported
        output_path: str
            Output file name
        export_format: str
            Data format for the data. Allowed values: fits|zarr

    Returns
    -------
        dask.Delayed

    Raises
    ------
        ValueError:
            If the provided data format is not in fits or zarr
    """

    if export_format == "fits":

        return export_to_fits(data, output_path)

    elif export_format == "zarr":

        data_to_export = data.copy(deep=False)
        data_to_export.attrs.clear()
        return data_to_export.to_zarr(
            store=f"{output_path}.zarr", compute=False
        )
    else:
        raise ValueError(f"Unsupported format: {export_format}")


@dask.delayed
def export_to_fits(image, output_path):
    """
    Export Image to fits

    Parameters
    ----------
        image: Image
            Image image to be exported
        output_path: str
            Output file name

    Returns
    -------
        None
    """
    new_hdu = fits.PrimaryHDU(
        data=image.pixels, header=image.image_acc.wcs.to_header()
    )
    new_hdu.writeto(f"{output_path}.fits")


def estimate_cell_size(
    baseline: float, wavelength: float, factor=3.0
) -> float:
    """
    A generalized function which estimates cell size for given baseline value.
    The baseline can be maximum value of U, V or W data.

    Parameters
    ----------
        baseline: float
            Baseline length in meters.
        wavelength: float
            Wavelength in meters.
            For better estimation, it has to be
            the minimum wavelength observed.
        factor: float
            Scaling factor.

    Returns
    -------
        float
            Cell size in arcsecond.
            **The output is rounded** to the 2 decimal places.
    """
    baseline /= wavelength

    cell_size_rad = 1.0 / (2.0 * factor * baseline)

    cell_size_arcsec = np.rad2deg(cell_size_rad) * 3600

    # Rounded to 2 decimals
    return cell_size_arcsec.round(2)


def estimate_image_size(
    maximum_wavelength: float, antenna_diameter: float, cell_size: float
) -> int:
    """
    Estimates dimension of the image which will be used in the imaging stage.

    Parameters
    ----------
        maximum_wavelength: float
            Maximum wavelength of the observation in meter.
        antenna_diameter: float
            Diameter of the antenna in meter.
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

    image_size = (1.5 * maximum_wavelength) / (
        cell_size_rad * antenna_diameter
    )

    # Rounding to the nearest multiple of 100
    return np.ceil(image_size / 100) * 100


# TODO: Untested function
def get_polarization(observation: xr.Dataset) -> PolarisationFrame:
    """
    Reads an observation from the xradio processing set,
    and extracts WCS information.
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


# TODO: get_image_metadata is untested function.
# Once stubbed imager is replaced by a proper imager
# we expect that the imager will give the image class instance
# with wcs information already populated.

# TODO: This does not handle MOMENT images, only FREQ
def get_wcs(observation, cell_size, nx, ny) -> WCS:
    """
    Reads an observation from the xradio processing set,
    and extracts WCS information.
    This is required to generate an instance of Image class.

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
    field_and_source_xds = observation.VISIBILITY.field_and_source_xds

    assert (
        field_and_source_xds.FIELD_PHASE_CENTER.units[0] == "rad"
    ), "Phase field center value is not defined in radian."
    assert (
        field_and_source_xds.FIELD_PHASE_CENTER.units[1] == "rad"
    ), "Phase field center value is not defined in radian."

    cell_size_degree = cell_size / 3600
    freq_channel_width = observation.frequency.channel_width["data"]
    ref_freq = observation.frequency.reference_frequency["data"]

    polarization_frame = get_polarization(observation)

    pol = PolarisationFrame.fits_codes[polarization_frame.type]
    npol = len(observation.polarization)
    if npol > 1:
        dpol = pol[1] - pol[0]
    else:
        dpol = 1.0

    fp_frame = field_and_source_xds.FIELD_PHASE_CENTER.frame.lower()
    # computes immediately
    fp_center = field_and_source_xds.FIELD_PHASE_CENTER.to_numpy()

    # TODO: Is the fp_frame equal to frame?
    coord = SkyCoord(
        ra=fp_center[0] * au.rad, dec=fp_center[1] * au.rad, frame=fp_frame
    )

    new_wcs = WCS(naxis=4)

    new_wcs.wcs.crpix = [nx // 2, ny // 2, 1, 1]
    new_wcs.wcs.cunit = ["deg", "deg", "", observation.frequency.units[0]]
    # computes immediately
    new_wcs.wcs.cdelt = np.array(
        [-cell_size_degree, cell_size_degree, dpol, freq_channel_width]
    )
    new_wcs.wcs.crval = [coord.ra.deg, coord.dec.deg, pol[0], ref_freq]
    new_wcs.wcs.ctype = ["RA---SIN", "DEC--SIN", "STOKES", "FREQ"]

    # TODO: "ICRS" since sdp-datamodels also have fixed radesys
    new_wcs.wcs.radesys = "ICRS"
    # new_wcs.wcs.radesys = coord.frame.name.upper()

    # TODO: "2000.0" since sdp-datamodels also have fixed equinox
    new_wcs.wcs.equinox = 2000.0
    # new_wcs.wcs.equinox = coord.frame.equinox.jyear

    # TODO: Verify this assignment is correct
    new_wcs.wcs.specsys = observation.frequency.frame

    return new_wcs
