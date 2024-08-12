# This pipeline additionally depends on ska_sdp_datamodels
# and ska_sdp_func_python
#
# Image the line free channels for the continuum model
# wsclean --size 256 256 --scale 60arcsec --pol IQUV <input.ms>
#
# Installing the pipeline
#
# poetry install
#
# Running the pipline
#
# spectral-line-imaging-pipeline --input <input.ms>
#
# With config overridden
# spectral-line-imaging-pipeline --input <input.ms> \
# --config spectral_line_imaging_pipeline.yaml
#
# pylint: disable=no-member,import-error
import logging
import shutil
from pathlib import Path

from dask_jobqueue import SLURMCluster

from ska_sdp_piper.piper.command import CLIArgument
from ska_sdp_piper.piper.constants import DEFAULT_CLI_ARGS
from ska_sdp_piper.piper.pipeline import Pipeline
from ska_sdp_piper.piper.utils import create_output_dir, read_yml, timestamp

from .diagnosis import SpectralLineDiagnoser
from .diagnosis.cli_arguments import DIAGNOSTIC_CLI_ARGS
from .stages.data_export import export_image, export_model, export_residual
from .stages.imaging import imaging_stage
from .stages.model import cont_sub, read_model, vis_stokes_conversion
from .stages.predict import predict_stage
from .stages.select_vis import select_field

logger = logging.getLogger()


spectral_line_imaging_pipeline = Pipeline(
    "spectral_line_imaging_pipeline",
    stages=[
        select_field,
        vis_stokes_conversion,
        read_model,
        predict_stage,
        export_model,
        cont_sub,
        imaging_stage,
        export_residual,
        export_image,
    ],
)


@spectral_line_imaging_pipeline.sub_command(
    "diagnose",
    DIAGNOSTIC_CLI_ARGS,
    help="Diagnose the pipeline",
)
def pipeline_diagnostic(cli_args):
    """
    Pipeline diagnostics sub_command

    Parameters
    ----------
        cli_args: argparse.Namespace
            CLI arguments
    """
    input_path = Path(cli_args.input)
    output_dir = "./diagnosis" if cli_args.output is None else cli_args.output

    timestamped_output_dir = Path(create_output_dir(output_dir, "pipeline-qa"))
    logger.info("==========================================")
    logger.info("=============== DIAGNOSE =================")
    logger.info("==========================================")
    logger.info(f"Current run output path : {timestamped_output_dir}")

    diagnoser = SpectralLineDiagnoser(
        input_path, timestamped_output_dir, cli_args.channel
    )
    diagnoser.diagnose()


@spectral_line_imaging_pipeline.sub_command(
    "run-on-cluster",
    DEFAULT_CLI_ARGS
    + [
        CLIArgument(
            "--cluster-config",
            dest="cluster_config",
            type=str,
            required=True,
            help="Path to cluster config file",
        )
    ],
    help="Create a SLURM job",
)
def run_on_cluster(cli_args):
    slip = spectral_line_imaging_pipeline
    cluster_config = read_yml(cli_args.cluster_config)

    cluster = SLURMCluster(**cluster_config)

    stages = [] if cli_args.stages is None else cli_args.stages[0]

    output_path = (
        "./output" if cli_args.output_path is None else cli_args.output_path
    )
    output_dir = create_output_dir(output_path, slip.name)

    cli_output_file = f"{output_dir}/{slip.name}_{timestamp()}.cli.yml"

    cluster_config_file = (
        f"{output_dir}/{slip.name}_{timestamp()}.cluster-config.yml"
    )

    slip._cli_command_parser.write_yml(cli_output_file)
    shutil.copyfile(cli_args.cluster_config, cluster_config_file)

    slip.run(
        cli_args.input,
        stages=stages,
        dask_scheduler=cluster,
        config_path=cli_args.config_path,
        verbose=(cli_args.verbose != 0),
        output_dir=output_dir,
        cli_args=slip._cli_command_parser.cli_args_dict,
    )
