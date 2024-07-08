# pylint: disable=import-error,no-name-in-module,no-member
import ducc0.wgridder
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
    )

    model = model.reshape(ntime, nbaseline)

    return model


def predict(ps, model_image, **kwargs):
    cell_size = kwargs["cell_size"]
    epsilon = kwargs["epsilon"]

    model_vec = xr.apply_ufunc(
        predict_ducc,
        ps.WEIGHT,
        ps.FLAG,
        ps.UVW,
        ps.frequency,
        input_core_dims=[
            ["time", "baseline_id"],
            ["time", "baseline_id"],
            ["time", "baseline_id", "uvw_label"],
            [],
        ],
        output_core_dims=[["time", "baseline_id"]],
        vectorize=True,
        kwargs=dict(
            model_image=model_image.values,
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
