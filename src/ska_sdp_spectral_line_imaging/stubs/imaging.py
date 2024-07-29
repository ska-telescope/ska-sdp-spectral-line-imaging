# pylint: disable=import-error,no-name-in-module,no-member
import ducc0.wgridder as wgridder
import numpy as np
import xarray as xr


def image_ducc(
    weight,
    flag,
    uvw,
    freq,
    vis,
    cell_size,
    nx,
    ny,
    epsilon,
    nchan,
    ntime,
    nbaseline,
):
    """
    Perform imaging using ducc0.gridder
    Parameters
    ----------
        weight: numpy.array
            Weights array
        flag: numpy.array
            Flag array
        uvw: numpy.array
            Polarization array
        freq: numpy.array
            Frequency array
        vis: numpy.array
            Visibility array
        cell_size: float
            Cell size in arcsecond
        nx: int
            Size of image X
        ny: int
            Size of image y
        epsilon: float
            Epsilon
        nchan: int
            Number of channel dimension
        ntime: int
            Number of time dimension
        nbaseline: int
            Number of baseline dimension
    Returns
    -------
        xarray.DataArray
    """

    # Note: There is a conversion to float 32 here
    vis_grid = vis.reshape(ntime * nbaseline, nchan).astype(np.complex64)
    uvw_grid = uvw.reshape(ntime * nbaseline, 3)
    weight_grid = weight.reshape(ntime * nbaseline, nchan).astype(np.float32)
    freq_grid = freq.reshape(nchan)

    dirty = wgridder.ms2dirty(
        uvw_grid,
        freq_grid,
        vis_grid,
        weight_grid,
        nx,
        ny,
        cell_size,
        cell_size,
        0,
        0,
        epsilon,
        nthreads=1
        #         mask=flag_xx
    )

    return xr.DataArray(dirty, dims=["ra", "dec"])


def cube_imaging(ps, cell_size, nx, ny, epsilon=1e-4):
    """
    Perform spectral cube imaging
    Parameters
    ----------
        ps: ProcessingSet
            Processing set
        cell_size: float
            Cell size
        nx: int
            Image size X
        ny: int
            Image size Y
        epsilon: float
            Epsilon
    """

    image_vec = xr.apply_ufunc(
        image_ducc,
        ps.WEIGHT,
        ps.FLAG,
        ps.UVW,
        ps.frequency,
        ps.VISIBILITY,
        input_core_dims=[
            ["time", "baseline_id"],
            ["time", "baseline_id"],
            ["time", "baseline_id", "uvw_label"],
            [],
            ["time", "baseline_id"],
        ],
        output_core_dims=[["ra", "dec"]],
        vectorize=True,
        kwargs=dict(
            nchan=1,
            ntime=ps.time.size,
            nbaseline=ps.baseline_id.size,
            cell_size=cell_size,
            epsilon=epsilon,
            nx=nx,
            ny=ny,
        ),
    )

    return xr.DataArray(
        image_vec.data,
        dims=["frequency", "polarization", "ra", "dec"],
    )
