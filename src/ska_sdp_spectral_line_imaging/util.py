def estimate_cell_size(uvw, frequency, nx, ny, factor=3.0):
    """
    Estimates cell size.

    Parameters
    ----------
        uvw: xarray.DataArray
            uvw data from the observation.
        frequency: float
            Reference frequency of the observation in Hz.
        nx: int
            Image size x
        ny: int
            Image size y
        factor: float
            Scaling factor.

    Returns
    -------
        float
        Cell size in arcsecond.

    """
    return NotImplemented
