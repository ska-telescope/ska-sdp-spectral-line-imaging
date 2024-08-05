from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage


@ConfigurableStage(
    "select_vis",
    configuration=Configuration(
        intent=ConfigParam(str, None),
        field_id=ConfigParam(int, 0),
        ddi=ConfigParam(int, 0),
    ),
)
def select_field(upstream_output, intent, field_id, ddi, _input_data_):
    """
    Selects the field from processing set

    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
        intent: str
            Name of the intent field
        field_id: int
            ID of the field in the processing set
        ddi: int
            Data description ID
        _input_data_: ProcessingSet
            Input processing set

    Returns
    -------
        dict
    """

    ps = _input_data_
    # TODO: This is a hack to get the psname
    psname = list(ps.keys())[0].split(".ps")[0]

    sel = f"{psname}.ps_ddi_{ddi}_intent_{intent}_field_id_{field_id}"

    # TODO: There is an issue in either xradio/xarray/dask that causes chunk
    # sizes to be different for coordinate variables
    return {"ps": ps[sel].unify_chunks()}
