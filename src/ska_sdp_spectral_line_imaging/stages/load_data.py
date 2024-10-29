from xradio.vis.read_processing_set import read_processing_set

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage


@ConfigurableStage(
    "load_data",
    configuration=Configuration(
        obs_id=ConfigParam(
            int, 0, "The index of the partition present in processing set"
        ),
    ),
)
def load_data(upstream_output, obs_id: int, _cli_args_):
    """
    Selects the field from processing set

    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
        obs_id: int
            The index of the partition present in processing set
        _cli_args_: dict
            CLI Arguments.

    Returns
    -------
        UpstreamOutput
    """
    input_path = _cli_args_["input"]
    ps = read_processing_set(ps_store=input_path)

    # computes
    sel = ps.summary().name[obs_id]

    # TODO: There is an issue in either xradio/xarray/dask that causes chunk
    # sizes to be different for coordinate variables
    selected_ps = ps[sel].unify_chunks()

    upstream_output["input_data"] = ps
    upstream_output["ps"] = selected_ps

    return upstream_output
