import logging
import os

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage
from ska_sdp_spectral_line_imaging.stubs.flagging import chunked_flagging

from .. import flagging_strategies

AOFLAGGER_AVAILABLE = True
try:
    import aoflagger  # noqa  # pylint: disable=unused-import
except ModuleNotFoundError:  # pragma: no cover
    AOFLAGGER_AVAILABLE = False  # pragma: no cover

logger = logging.getLogger()


@ConfigurableStage(
    "flagging",
    configuration=Configuration(
        strategy_file=ConfigParam(
            str,
            None,
            description="Path to the flagging strategy file (.lua)",
        ),
    ),
)
def flagging_stage(
    upstream_output,
    strategy_file,
):
    """
    Perfoms flagging on visibilities using strategies and existing flags.

    Parameters
    -----------
        upstream_output: UpstreamOutput
            Output from the upstream stage
        strategy_file: str
            Path to the flagging strategy file

    Returns
    -------
        UpstreamOutput

    Raises
    ------
        FileNotFoundError
            If a .lua for applying strategies is not found
    """
    if not AOFLAGGER_AVAILABLE:
        raise ImportError("Unable to import aoflagger")

    if strategy_file is None:
        logger.info(
            "Strategy file is not provided. "
            "Picking up the default strategy file for flagging."
        )
        strategy_path = os.path.dirname(
            os.path.abspath(flagging_strategies.__file__)
        )
        strategy_file = f"{strategy_path}/generic-default.lua"
    else:
        if not os.path.exists(strategy_file):
            raise FileNotFoundError(
                f"The provided strategy file path {strategy_file} "
                f"does not not exists"
            )
    logger.info(f"The strategy file picked up for flagging: {strategy_file}")

    ps = upstream_output.ps
    ntime = ps.VISIBILITY.time.size
    nchan = ps.VISIBILITY.frequency.size
    npol = ps.VISIBILITY.polarization.size

    visibility_rechunked = ps.VISIBILITY.chunk(
        ({"baseline_id": 1, "frequency": nchan})
    )
    flag_rechunked = ps.FLAG.chunk(({"baseline_id": 1, "frequency": nchan}))
    flagged_cube = chunked_flagging(
        visibility_rechunked, flag_rechunked, ntime, nchan, npol, strategy_file
    )

    flagged_values = (
        flagged_cube.expand_dims(dim={"polarization": ps.FLAG.polarization})
        .transpose(*ps.FLAG.dims)
        .chunk(ps.FLAG.chunksizes)
    )

    ps = ps.assign({"FLAG": flagged_values})
    upstream_output["ps"] = ps

    return upstream_output
