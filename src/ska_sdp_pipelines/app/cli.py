from typer import Typer

from .executable_pipeline import ExecutablePipeline

app = Typer()


@app.command()
def install(pipeline_path):
    """
    Pipeline framework command to install pipelines
    Parameter:
       pipeline_path: Path to the pipeline to be installed
    """

    executable_pipeline = ExecutablePipeline(pipeline_path)
    executable_pipeline.validate_pipeline()
    executable_pipeline.prepare_executable()
    executable_pipeline.install()


@app.command()
def uninstall(pipeline_path):
    """
    Pipeline framework command to uninstall pipelines
    Parameter:
       pipeline_path: Path to the pipeline to be uninstalled
    """
    executable_pipeline = ExecutablePipeline(pipeline_path)
    executable_pipeline.validate_pipeline()
    executable_pipeline.prepare_executable()
    executable_pipeline.uninstall()
