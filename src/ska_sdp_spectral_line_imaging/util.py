import numpy as np


# TODO: Should this function take max_freq or min_wavelength?
def estimate_cell_size(umax: float, frequency: float, factor=3.0) -> float:
    """
    A generalized function which estimates cell size for given "umax" value.
    Here, "umax" can either be maximum value of U, V or W data.

    Parameters
    ----------
        umax: float
            Maximum value from uvw data from the observation.
        frequency: float
            Frequency in Hz.
            For better estimation, it has to be the maximum frequency observed.
        factor: float
            Scaling factor.

    Returns
    -------
        float
            cell size in arcsecond.
    """
    wave_length = 3.0e8 / frequency

    umax /= wave_length

    cell_size_rad = 1.0 / (2.0 * factor * umax)

    cell_size_arcsec = np.rad2deg(cell_size_rad) * 3600

    return cell_size_arcsec


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
            Size of the image
            Note: The output is rounded to the nearest multiple of 100
            greater than the calculated image size.
    """
    cell_size_rad = np.deg2rad(cell_size / 3600)

    image_size = (1.5 * maximum_wavelength) / (
        cell_size_rad * antenna_diameter
    )

    # Rounding to the nearest multiple of 100
    return np.ceil(image_size / 100) * 100
