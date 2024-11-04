import logging
import os
import subprocess

from typer import Context, Option, Typer

from .. import scripts
from ..piper.utils import LogUtil
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
    output_path=Option(
        default="./benchmark",
        help="""Output folder to store the results.""",
    ),
    capture_interval=Option(
        default=5,
        help="""Time interval for catpuring benchmarking stats""",
    ),
):
    """
    Pipeline framework command to install pipelines

    Parameters
    ----------
        command: str
            Name of the pipeline to be benchmarked
        setup: bool
            Setup dool
        output_path: str
            Output folder to store the benchmark results
        capture_interval: int
            Time interval for capturing benchmarking stats
        ctx: Context
            Command context

    """

    if not setup and command is None:
        ctx.command.get_help(ctx)
        return

    script_path = os.path.dirname(os.path.abspath(scripts.__file__))

    if setup:
        if not os.path.exists(f"{script_path}/dool"):
            subprocess.run(
                [
                    f"{script_path}/setup-dool.sh",
                    f"{script_path}/dool",
                ]
            )

    if command is not None:
        if not os.path.exists(f"{script_path}/dool"):
            raise RuntimeError(
                "Dool not found, please run with `--setup` option"
            )

        subprocess.run(
            [
                f"{script_path}/run-dool.sh",
                output_path,
                command.split(" ")[0],
                command,
            ],
            env={
                "DOOL_BIN": f"{script_path}/dool/dool",
                "PATH": os.getenv("PATH"),
                "DELAY_IN_SECONDS": capture_interval,
            },
        )
