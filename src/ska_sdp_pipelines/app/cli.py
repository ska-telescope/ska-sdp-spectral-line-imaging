from typer import Option, Typer

from .executable_pipeline import ExecutablePipeline

app = Typer()


@app.command()
def install(
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

    executable_pipeline = ExecutablePipeline(pipeline_path)
    executable_pipeline.validate_pipeline()
    executable_pipeline.prepare_executable()
    executable_pipeline.install(config_install_path)


@app.command()
def uninstall(pipeline_path):
    """
    Pipeline framework command to uninstall pipelines

    Parameters
    ---------
       pipeline_path: str
            Path to the pipeline to be uninstalled
    """
    executable_pipeline = ExecutablePipeline(pipeline_path)
    executable_pipeline.validate_pipeline()
    executable_pipeline.prepare_executable()
    executable_pipeline.uninstall()
