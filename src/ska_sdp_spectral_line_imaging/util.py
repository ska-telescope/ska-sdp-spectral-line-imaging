import numpy as np


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
