import os

from ska_sdp_piper.piper.configurable_stage import ConfigurableStage
from ska_sdp_piper.piper.configuration import ConfigParam, Configuration


@ConfigurableStage(
    "export_residual",
    Configuration(
        psout_name=ConfigParam(str, "residual.zarr"),
    ),
)
def export_residual(upstream_output, psout_name, _output_dir_):
    """
    Export continuum subtracted residual

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        psout_name: str
            Output file name
        _output_dir_: str
            Output directory created for the run

    Returns
    -------
        upstream_output
    """

    ps = upstream_output["ps"]
    output_path = os.path.abspath(os.path.join(_output_dir_, psout_name))
    ps.VISIBILITY.to_zarr(store=output_path)
    return upstream_output


@ConfigurableStage(
    "export_model",
    Configuration(
        psout_name=ConfigParam(str, "model.zarr"),
    ),
)
def export_model(upstream_output, psout_name, _output_dir_):
    """
    Export predicted model

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        psout_name: str
            Output file name
        _output_dir_: str
            Output directory created for the run

    Returns
    -------
        upstream_output
    """
    model_vis = upstream_output["model_vis"]
    output_path = os.path.abspath(os.path.join(_output_dir_, psout_name))
    model_vis.to_zarr(store=output_path)
    return upstream_output


@ConfigurableStage(
    "export_zarr",
    Configuration(
        image_name=ConfigParam(str, "output_image.zarr"),
    ),
)
def export_image(upstream_output, image_name, _output_dir_):
    """
    Export the generated cube image

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        image_name: str
            Output file name
        _output_dir_: str
            Output directory created for the run

    Returns
    -------
        upstream_output
    """
    cubes = upstream_output["cubes"]
    output_path = os.path.join(_output_dir_, image_name)

    cubes.to_zarr(store=output_path)
    return upstream_output
