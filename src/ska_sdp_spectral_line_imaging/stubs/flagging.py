import xarray as xr

AOFLAGGER_AVAILABLE = True
try:
    import aoflagger
except ModuleNotFoundError:  # pragma: no cover
    AOFLAGGER_AVAILABLE = False  # pragma: no cover


def flag_baseline(visibility, flags, ntime, nchan, npol, strategy_file):
    """
    Perform flagging using AOFlagger

    Parameters
    ----------
        visibility: numpy array
            visibility array
        flags: numpy array
            flags array
        ntime: int
            no. of timesteps
        nchan: int
            no. of channels
        npol: int
            no. of polarizations
        strategy_file: str
            strategy file to apply flagging

    Returns
    -------
        flagged_cube: numpy array
            Array of flags
    """
    if not AOFLAGGER_AVAILABLE:
        raise ImportError("Unable to import aoflagger")
    flagger = aoflagger.AOFlagger()
    strategy_path = strategy_file

    strategy = flagger.load_strategy_file(strategy_path)
    data = flagger.make_image_set(ntime, nchan, npol * 2)
    for pol_idx in range(npol):
        pol_data = visibility[pol_idx]
        real = pol_data.real
        imag = pol_data.imag
        data.set_image_buffer(pol_idx * 2, real)
        data.set_image_buffer(pol_idx * 2 + 1, imag)

    mask = flagger.make_flag_mask(ntime, nchan, False)
    mask.set_buffer(flags[0])

    flags = strategy.run(data)
    return flags.get_buffer()


def chunked_flagging(visibility, flags, ntime, nchan, npol, strategy_file):
    """
    Perform flagging on individual chunks.

    Parameters
    ----------
        visibility: xarray.DataArray
            visibility array
        flags: xarray.DataArray
            flags array
        ntime: int
            no. of timesteps
        nchan: int
            no. of channels
        npol: int
            no. of polarizations
        strategy_file: str
            strategy file to apply flagging

    Returns
    -------
        flagged_cube: xarray.DataArray
            Array of flags
    """
    flagged_cube = xr.apply_ufunc(
        flag_baseline,
        visibility,
        flags,
        input_core_dims=[
            ["polarization", "frequency", "time"],
            ["polarization", "frequency", "time"],
        ],
        output_core_dims=[["frequency", "time"]],
        vectorize=True,
        keep_attrs=True,
        output_dtypes=[bool],
        dask="parallelized",
        dask_gufunc_kwargs={
            "output_sizes": {"time": ntime, "frequency": nchan},
        },
        kwargs=dict(
            ntime=ntime, nchan=nchan, npol=npol, strategy_file=strategy_file
        ),
    )
    return flagged_cube
