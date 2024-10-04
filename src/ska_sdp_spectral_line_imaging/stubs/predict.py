# pylint: disable=import-error,no-name-in-module,no-member
import ducc0.wgridder
import numpy as np
import xarray as xr


def predict_ducc(
    weight,
    flag,
    uvw,
    freq,
    model_image,
    cell_size,
    epsilon,
    nchan,
    ntime,
    nbaseline,
):
    """
    Perform prediction using ducc0.gridder

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
        model_image: numpy.array
            Model image
        cell_size: float
            Cell size in radians
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

    uvw_grid = uvw.reshape(ntime * nbaseline, 3)
    weight_grid = weight.reshape(ntime * nbaseline, nchan)
    freq_grid = freq.reshape(nchan)

    model = ducc0.wgridder.dirty2ms(
        uvw_grid,
        freq_grid,
        model_image,
        weight_grid,
        cell_size,
        cell_size,
        0,
        0,
        epsilon,
        nthreads=1,
    )

    model = model.reshape(ntime, nbaseline)

    return model


def predict(ps, model_image, **kwargs):
    """
    Predict model column for processing set

    Parameters
    ----------
        ps: ProcessingSet
            Processing set
        model_image: numpy.array
            Model image
        **kwargs:
            Additional keyword arguments

    Returns
    -------
        xarray.DataArray
    """

    cell_size = kwargs["cell_size"]
    epsilon = kwargs["epsilon"]

    model_vec = xr.apply_ufunc(
        predict_ducc,
        ps.WEIGHT,
        ps.FLAG,
        ps.UVW,
        ps.frequency,
        model_image,
        input_core_dims=[
            ["time", "baseline_id"],
            ["time", "baseline_id"],
            ["time", "baseline_id", "uvw_label"],
            [],
            ["ra", "dec"],
        ],
        output_core_dims=[["time", "baseline_id"]],
        vectorize=True,
        kwargs=dict(
            nchan=1,
            ntime=ps.time.size,
            nbaseline=ps.baseline_id.size,
            cell_size=cell_size,
            epsilon=epsilon,
        ),
    )

    return xr.DataArray(
        model_vec.data,
        dims=["frequency", "polarization", "time", "baseline_id"],
    )


def predict_for_channels(ps, model_image, epsilon, cell_size):
    template_core_dims = ["frequency", "polarization", "time", "baseline_id"]
    template_chunk_sizes = {
        k: v for k, v in ps.chunksizes.items() if k in template_core_dims
    }
    output_xr = xr.DataArray(
        np.empty(
            (
                ps.sizes["frequency"],
                ps.sizes["polarization"],
                ps.sizes["time"],
                ps.sizes["baseline_id"],
            ),
            dtype=np.complex64,
        ),
        dims=template_core_dims,
    ).chunk(template_chunk_sizes)

    cell_size_radian = np.deg2rad(cell_size / 3600)

    predicted_visibility = xr.map_blocks(
        predict,
        ps,
        template=output_xr,
        kwargs=dict(
            model_image=model_image,
            epsilon=epsilon,
            cell_size=float(cell_size_radian),
        ),
    )

    predicted_visibility = predicted_visibility.assign_coords(
        ps.VISIBILITY.coords
    )

    return predicted_visibility
