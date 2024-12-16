import logging
import os

from typer import Context, Option, Typer

from ..piper.utils import LogUtil
from .benchmark import DOOL_BIN, run_dool, setup_dool
from .executable_pipeline import ExecutablePipeline

logger = logging.getLogger(__name__)

app = Typer(no_args_is_help=True, add_completion=True)


@app.command()
def install(
    pipeline_name,
    pipeline_path,
    config_install_path=Option(
        default=None,
        help="""Path to place the defualt config. If not provided,
            the config will be saved into the pipeline script path""",
    ),
):
    """
    Pipeline framework command to install pipelines

    Parameters
    ----------
       pipeline_path: str
           Path to the pipeline to be installed
       config_install_path: str
           Path to place the default config.
    """

    LogUtil.configure()
    logger.info("=============== START INSTALL =====================")
    executable_pipeline = ExecutablePipeline(pipeline_name, pipeline_path)
    executable_pipeline.validate_pipeline()
    executable_pipeline.prepare_executable()
    executable_pipeline.install(config_install_path)
    logger.info("=============== FINISH INSTALL ====================")


@app.command()
def uninstall(pipeline_name, pipeline_path):
    """
    Pipeline framework command to uninstall pipelines

    Parameters
    ---------
       pipeline_path: str
            Path to the pipeline to be uninstalled
    """

    LogUtil.configure()
    logger.info("=============== START UNINSTALL =====================")
    executable_pipeline = ExecutablePipeline(pipeline_name, pipeline_path)
    executable_pipeline.validate_pipeline()
    executable_pipeline.prepare_executable()
    executable_pipeline.uninstall()
    logger.info("=============== FINISH UNINSTALL ====================")


@app.command()
def benchmark(
    ctx: Context,
    command: str = Option(
        default=None,
        help="""Command to be benchmarked. If command includes
            arguments and options, enclose the entire command
            in double quotes (\")""",
    ),
    setup: bool = Option(
        default=False,
        is_flag=True,
        help="""Setup dool for benchmarking""",
    ),
    output_path: str = Option(
        default="./benchmark",
        help="""Output folder to store the results.""",
    ),
    capture_interval: int = Option(
        default=5,
        help="""Time interval for catpuring benchmarking stats""",
    ),
    output_file_prefix: str = Option(
        default=None,
        help="""Output file name prefix""",
    ),
):
    """
    Pipeline framework command to install pipelines

    Parameters
    ----------
        ctx: Context
            Command context
        command: str
            Name of the pipeline to be benchmarked
        setup: bool
            Setup dool
        output_path: str
            Output folder to store the benchmark results
        capture_interval: int
            Time interval for capturing benchmarking stats
        output_file_prefix: str
            Output File prefix name
    """

    if not setup and command is None:
        ctx.command.get_help(ctx)
        return

    output_prefix = (
        "" if output_file_prefix is None else f"{output_file_prefix}_"
    )

    if setup:
        if not os.path.exists(DOOL_BIN):
            setup_dool()

    if command is not None:
        if not os.path.exists(DOOL_BIN):
            raise RuntimeError(
                "Dool not found, please run with `--setup` option"
            )

        command_list = command.split(" ")

        run_dool(
            output_dir=output_path,
            file_prefix=f"{output_prefix}{command_list[0]}",
            command=command_list,
            capture_interval=capture_interval,
        )
