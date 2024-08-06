from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage


@ConfigurableStage(
    "select_vis",
    configuration=Configuration(
        obs_mode=ConfigParam(list(), 0),
    ),
)
def select_field(upstream_output, obs_mode, _input_data_):
    """
    Selects the field from processing set

    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
        obs_mode: list
            List of obervational modes
        _input_data_: ProcessingSet
            Input processing set

    Returns
    -------
        dict
    """

    ps = _input_data_
    # TODO: This is a hack to get the psname
    psname = list(ps.keys())[0].split(".ps")[0]

    sel = f"{psname}.ps_{obs_mode}"

    # TODO: There is an issue in either xradio/xarray/dask that causes chunk
    # sizes to be different for coordinate variables
    return {"ps": ps[sel].unify_chunks()}
