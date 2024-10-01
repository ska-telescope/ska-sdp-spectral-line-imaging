import numpy as np


def estimate_cell_size(uvw, frequency, factor=3.0):
    """
    Estimates cell size.

    Parameters
    ----------
        uvw: xarray.DataArray
            uvw data from the observation.
        frequency: float
            Reference frequency of the observation in Hz.
        factor: float
            Scaling factor.

    Returns
    -------
        numpy.ndarray
        U and V cell sizes in arcsecond.

    """
    umax, vmax, _ = np.abs(uvw).max(dim=["time", "baseline_id"])

    wave_length = 3.0e8 / frequency

    umax /= wave_length
    vmax /= wave_length

    u_cell_size = 1.0 / (2.0 * factor * umax)
    v_cell_size = 1.0 / (2.0 * factor * vmax)

    return np.rad2deg([u_cell_size, v_cell_size]) * 3600
