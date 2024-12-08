# pylint: disable=no-member,import-error
import logging
from pathlib import Path

from ska_sdp_piper.piper.command.cli_command_parser import CLIArgument
from ska_sdp_piper.piper.pipeline import Pipeline
from ska_sdp_piper.piper.stage import Stages
from ska_sdp_piper.piper.utils import create_output_dir

from .diagnosis.cli_arguments import DIAGNOSTIC_CLI_ARGS
from .diagnosis.spectral_line_diagnoser import SpectralLineDiagnoser
from .scheduler import DefaultScheduler
from .stages.flagging import flagging_stage
from .stages.imaging import imaging_stage
from .stages.load_data import load_data
from .stages.model import cont_sub, read_model, vis_stokes_conversion
from .stages.predict import predict_stage

logger = logging.getLogger()

scheduler = DefaultScheduler()

spectral_line_imaging_pipeline = Pipeline(
    "spectral_line_imaging_pipeline",
    stages=Stages(
        [
            load_data,
            vis_stokes_conversion,
            read_model,
            predict_stage,
            cont_sub,
            flagging_stage,
            imaging_stage,
        ]
    ),
    cli_args=[
        CLIArgument(
            "--input",
            dest="input",
            type=str,
            required=True,
            help="Path to input processing set",
        ),
    ],
    scheduler=scheduler,
)


@spectral_line_imaging_pipeline.sub_command(
    DIAGNOSTIC_CLI_ARGS,
    help="""
    Run diagnostics on the pipeline output products.
    This will generate various graphs from the output
    visibitlies of the pipeline
    (model and residual after continuum subtraction).
    The graphs are stores as PNG images in the specified output
    directory.
    """,
)
def diagnose(input, channel, dask_scheduler=None, output=None, **kwargs):
    """
    Run diagnostics on the pipeline output products.
    This will generate various graphs from the output
    visibitlies of the pipeline
    (model and residual after continuum subtraction).
    The graphs are stores as PNG images in the specified output
    directory.

    This can be run from cli as "diagnose" subcommand.

    Parameters
    ----------
        input: str, or os.PathLike
            Path to the directory containing the output
            products of the pipeline.
        channel: int
            A line free channel to plot uv distances.
        dask_scheduler: str, or Cluster, optional
            An IP address of the scheduler, or an instance of a dask
            Cluster, which will be passed to a dask Client. If not
            provided, then the pipeline will run locally on the with
            multi-threading.
        output: str, or os.PathLike, optional
            Path to the directory to place the diagnosis
            output products. If not provided, a new directory called
            "diagnosis" will be created in current working directory.
            Actual output products will be stored in a new timestamped
            directory inside the output directory.
        kwargs: dict
            A dictionary to hold any additional kwargs

    Returns
    -------
        None
    """
    input_path = Path(input)
    output_dir = output or "./diagnosis"

    timestamped_output_dir = Path(create_output_dir(output_dir, "pipeline-qa"))
    logger.info("==========================================")
    logger.info("=============== DIAGNOSE =================")
    logger.info("==========================================")
    logger.info(f"Current run output path : {timestamped_output_dir}")

    diagnoser = SpectralLineDiagnoser(
        input_path,
        timestamped_output_dir,
        channel,
        dask_scheduler,
        scheduler=scheduler,
    )

    diagnoser.diagnose()
