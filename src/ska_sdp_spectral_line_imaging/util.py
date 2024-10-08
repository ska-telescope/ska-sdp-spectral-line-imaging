import numpy as np


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
