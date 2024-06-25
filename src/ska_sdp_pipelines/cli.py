from typer import Typer

app = Typer()


@app.command()
def install(pipeline):
    """
    Pipeline framework command to install pipelines
    Parameter:
       pipeline: URL to the pipeline to be installed
    """
    print(pipeline)
