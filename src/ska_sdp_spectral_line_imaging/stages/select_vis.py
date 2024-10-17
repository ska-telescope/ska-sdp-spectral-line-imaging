from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage


@ConfigurableStage(
    "select_vis",
    configuration=Configuration(
        obs_id=ConfigParam(
            int, 0, "The index of the partition present in processing set"
        ),
    ),
)
# TODO: This is always supposed to be the first stage in the pipeline.
# Can we make it unskippable?
def select_field(upstream_output, obs_id: int, _input_data_):
    """
    Selects the field from processing set

    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
        obs_id: int
            The index of the partition present in processing set
        _input_data_: ProcessingSet
            Input processing set

    Returns
    -------
        dict
    """
    ps = _input_data_
    sel = ps.summary().name[obs_id]

    # TODO: There is an issue in either xradio/xarray/dask that causes chunk
    # sizes to be different for coordinate variables
    selected_ps = ps[sel].unify_chunks()
    upstream_output["ps"] = selected_ps

    return upstream_output
