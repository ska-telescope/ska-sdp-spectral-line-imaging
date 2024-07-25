import logging

from typer import Option, Typer

from ..framework.log_util import LogUtil
from .executable_pipeline import ExecutablePipeline

logger = logging.getLogger(__name__)

app = Typer()


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

    LogUtil.configure(pipeline_name)
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

    LogUtil.configure(pipeline_name)
    logger.info("=============== START UNINSTALL =====================")
    executable_pipeline = ExecutablePipeline(pipeline_name, pipeline_path)
    executable_pipeline.validate_pipeline()
    executable_pipeline.prepare_executable()
    executable_pipeline.uninstall()
    logger.info("=============== FINISH UNINSTALL ====================")
