import logging
import os

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..data_procs.flagging import flag_cube
from ..data_procs.flagging_strategies import FlaggingStrategy
from ..util import export_to_zarr, rechunk

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
        flagging_configs=ConfigParam(
            dict,
            dict(
                base_threshold=1.0,
                iteration_count=3,
                threshold_factor_step=2.0,
                transient_threshold_factor=4.0,
                exclude_original_flags=True,
                threshold_timestep_rms=9.0,
                threshold_channel_rms=9.0,
                flag_low_outliers=True,
                include_low_pass=True,
                window_size=[21, 31],
                time_sigma=2.5,
                freq_sigma=3.0,
            ),
            description="Path to the flagging strategy file (.lua)",
        ),
        export_flags=ConfigParam(
            bool,
            False,
            description="Export the Flags",
        ),
        psout_name=ConfigParam(
            str,
            "flags",
            description="Output path of flags",
        ),
    ),
)
def flagging_stage(
    upstream_output,
    strategy_file,
    flagging_configs,
    export_flags,
    psout_name,
    _output_dir_,
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
            "Strategy file is not provided. Building the default"
            " strategy file for flagging using the configs."
        )

        strategy_file = f"{_output_dir_}/default_strategy.lua"

        strategy = FlaggingStrategy(**flagging_configs)
        strategy.write(strategy_file)

    else:
        if not os.path.exists(strategy_file):
            raise FileNotFoundError(
                f"The provided strategy file path {strategy_file} "
                f"does not not exists"
            )
    logger.info(f"The strategy file picked up for flagging: {strategy_file}")

    ps = upstream_output.ps

    flagged_cube = flag_cube(ps, strategy_file)

    flagged_values = rechunk(
        flagged_cube, ps.FLAG, dim={"polarization": ps.FLAG.polarization}
    )

    ps = ps.assign({"FLAG": flagged_values})

    if export_flags:
        output_path = os.path.join(_output_dir_, psout_name)
        upstream_output.add_compute_tasks(
            export_to_zarr(ps.FLAG, output_path, clear_attrs=True)
        )

    upstream_output["ps"] = ps

    return upstream_output
