import os

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage


@ConfigurableStage(
    "export_residual",
    Configuration(
        psout_name=ConfigParam(str, "vis_residual.zarr"),
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

    ps = upstream_output["continuum_subtraction"]
    output_path = os.path.abspath(os.path.join(_output_dir_, psout_name))
    ps.VISIBILITY.to_zarr(store=output_path)


@ConfigurableStage(
    "export_model",
    Configuration(
        psout_name=ConfigParam(str, "vis_model.zarr"),
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
    ps = upstream_output["predict_stage"]
    output_path = os.path.abspath(os.path.join(_output_dir_, psout_name))
    ps.VISIBILITY_MODEL.attrs.clear()
    ps.VISIBILITY_MODEL.to_zarr(store=output_path)


@ConfigurableStage(
    "export_image",
    Configuration(
        image_name=ConfigParam(str, "spectral_cube.zarr"),
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
    cube = upstream_output["imaging"]
    output_path = os.path.join(_output_dir_, image_name)

    cube.to_zarr(store=output_path)
