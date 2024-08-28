import xarray as xr
from ska_sdp_datamodels.science_data_model.polarisation_functions import (
    convert_pol_frame,
)
from ska_sdp_datamodels.science_data_model.polarisation_model import (
    PolarisationFrame,
)

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage


@ConfigurableStage(
    "select_vis",
    configuration=Configuration(
        obs_mode=ConfigParam(list(), 0),
        do_stokes_conversion=ConfigParam(bool, False),
    ),
)
def select_field(
    upstream_output, obs_mode, do_stokes_conversion, _input_data_
):
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
    ps = ps[sel].unify_chunks()

    if do_stokes_conversion:
        converted_vis = xr.apply_ufunc(
            convert_pol_frame,
            ps.VISIBILITY,
            kwargs=dict(
                ipf=PolarisationFrame("linear"),
                opf=PolarisationFrame("stokesIQUV"),
                polaxis=3,
            ),
            dask="allowed",
        )
        ps = ps.assign(dict(VISIBILITY=converted_vis))

    return ps
