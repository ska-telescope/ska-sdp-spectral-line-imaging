# pylint: disable=import-error,no-name-in-module,no-member
import ducc0.wgridder
import numpy as np
import xarray as xr


def _apply_power_law_scaling(
    image: np.ndarray,
    frequency_of_current_chunk: np.ndarray | float,
    reference_frequency: float = None,
    spectral_index: float = 0.75,
):
    channel_multiplier = np.power(
        (frequency_of_current_chunk / reference_frequency[0]), -spectral_index
    )

    return image * channel_multiplier


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
    image_type,
    ref_freq,
    spectral_index,
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
        image_type: str
            continuum | spectral
        ref_freq: float
        spectral_index: float

    Returns
    -------
        xarray.DataArray
    """

    uvw_grid = uvw.reshape(ntime * nbaseline, 3)
    weight_grid = weight.reshape(ntime * nbaseline, nchan)
    freq_grid = freq.reshape(nchan)

    if image_type == "continuum":
        model_image = _apply_power_law_scaling(
            model_image,
            freq,
            ref_freq,
            spectral_index=spectral_index,
        )

    model_image = model_image.astype(np.float32)

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
            ["y", "x"],
        ],
        output_core_dims=[["time", "baseline_id"]],
        vectorize=True,
        keep_attrs=True,
        dask="parallelized",
        # TODO: parameterize dtype
        output_dtypes=[np.complex64],
        kwargs=dict(
            nchan=1,
            ntime=ps.time.size,
            nbaseline=ps.baseline_id.size,
            **kwargs
        ),
    )

    return model_vec


def predict_for_channels(
    ps,
    model_image,
    epsilon,
    cell_size,
    image_type=None,
    ref_freq=None,
    spectral_index=None,
):

    cell_size_radian = np.deg2rad(cell_size / 3600)

    # TODO: Remove once "predict" function can accept any dtypes
    model_image = model_image.astype(np.float32)

    predicted_visibility = predict(
        ps,
        model_image,
        epsilon=epsilon,
        cell_size=cell_size_radian,
        image_type=image_type,
        ref_freq=ref_freq,
        spectral_index=spectral_index,
    )

    predicted_visibility = predicted_visibility.assign_coords(
        ps.VISIBILITY.coords
    )

    return predicted_visibility
