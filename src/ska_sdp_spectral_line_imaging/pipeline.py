# This pipeline additionally depends on ska_sdp_datamodels
# and ska_sdp_func_python
#
# Image the line free channels for the continuum model
# wsclean --size 256 256 --scale 60arcsec --pol IQUV <input.ms>
#
# Installing the pipeline
#
# sdp-pipeline install pipeline.py
#
# Running the pipline
#
# spectral_line_imaging_pipeline --input <input.ms>
#
# With config overridden
# spectral_line_imaging_pipeline --input <input.ms> \
# --config spectral_line_imaging_pipeline.yaml
#
# pylint: disable=no-member,import-error
from pathlib import Path

from ska_sdp_piper.piper.pipeline import Pipeline
from ska_sdp_piper.piper.utils import create_output_dir

from .diagnosis import SpectralLineDiagnoser
from .diagnosis.cli_arguments import DIAGNOSTIC_CLI_ARGS
from .stages.data_export import export_image, export_model, export_residual
from .stages.imaging import imaging_stage
from .stages.model import cont_sub, read_model, vis_stokes_conversion
from .stages.predict import predict_stage
from .stages.select_vis import select_field

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
    input_path = Path(cli_args.input)
    output_dir = "./diagnosis" if cli_args.output is None else cli_args.output

    timestamped_output_dir = Path(create_output_dir(output_dir, "pipeline-qa"))
    diagnoser = SpectralLineDiagnoser(input_path, timestamped_output_dir)
    diagnoser.diagnose()
