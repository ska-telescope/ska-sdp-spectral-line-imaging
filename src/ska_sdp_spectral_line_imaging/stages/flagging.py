import logging
import os

from ska_sdp_piper.piper.configurations import (
    ConfigParam,
    Configuration,
    NestedConfigParam,
)
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
            description="Path to the flagging strategy file (.lua)."
            " If null, a default strategy will be built using"
            " strategy_configs.",
        ),
        strategy_configs=NestedConfigParam(
            "Configurations to build the default strategy file",
            base_threshold=ConfigParam(
                float,
                2.0,
                description="Flagging sensitivity threshold."
                " Lower means more sensitive detection",
                nullable=False,
            ),
            iteration_count=ConfigParam(
                int,
                3,
                description="Number of flagging iterations",
                nullable=False,
            ),
            threshold_factor_step=ConfigParam(
                float,
                4.0,
                description="How much to increase the sensitivity"
                " each iteration",
                nullable=False,
            ),
            transient_threshold_factor=ConfigParam(
                float,
                5.0,
                description="Transient RFI threshold. Decreasing this value"
                " makes detection of transient RFI more aggressive",
                nullable=False,
            ),
            threshold_timestep_rms=ConfigParam(
                float,
                3.0,
                description="RMS sigma threshold for time domain",
                nullable=False,
            ),
            threshold_channel_rms=ConfigParam(
                float,
                3.0,
                description="RMS sigma threshold for frequency domain",
                nullable=False,
            ),
            keep_original_flags=ConfigParam(
                bool,
                True,
                description="Consider the original flags while applying"
                " strategy",
                nullable=False,
            ),
            keep_outliers=ConfigParam(
                bool,
                True,
                description="Keep frequency outliers during channel"
                " rms threshold.",
                nullable=False,
            ),
            low_pass_filter=NestedConfigParam(
                "Low pass filter configs",
                do_low_pass=ConfigParam(
                    bool,
                    True,
                    description="Do low pass filtering",
                    nullable=False,
                ),
                window_size=ConfigParam(
                    list,
                    [11, 21],
                    description="Kernel size for low pass filtering",
                    nullable=False,
                ),
                time_sigma=ConfigParam(
                    float,
                    6.0,
                    description="Sigma threshold for time domain",
                    nullable=False,
                ),
                freq_sigma=ConfigParam(
                    float,
                    7.0,
                    description="Sigma threshold for frequency domain",
                    nullable=False,
                ),
            ),
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
    strategy_configs,
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

        configs = {**strategy_configs, **strategy_configs["low_pass_filter"]}

        strategy = FlaggingStrategy(**configs)
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
