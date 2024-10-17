import os

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..util import export_to_fits


@ConfigurableStage(
    "export_residual",
    Configuration(
        psout_name=ConfigParam(
            str, "vis_residual", "Output path of residual data"
        ),
    ),
)
def export_residual(upstream_output, psout_name, _output_dir_):
    """
    Export continuum subtracted residual in zarr format.

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

    ps = upstream_output.ps
    output_path = os.path.join(_output_dir_, psout_name)
    ps.VISIBILITY.attrs.clear()
    export_residual = ps.VISIBILITY.to_zarr(
        store=f"{output_path}.zarr", compute=False
    )
    upstream_output.add_compute_tasks(export_residual)

    return upstream_output


@ConfigurableStage(
    "export_model",
    Configuration(
        psout_name=ConfigParam(str, "vis_model", "Output path of model data"),
    ),
)
def export_model(upstream_output, psout_name, _output_dir_):
    """
    Export predicted model in zarr format.

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
    ps = upstream_output.ps
    output_path = os.path.join(_output_dir_, psout_name)
    ps.VISIBILITY_MODEL.attrs.clear()
    export_model = ps.VISIBILITY_MODEL.to_zarr(
        store=f"{output_path}.zarr", compute=False
    )
    upstream_output.add_compute_tasks(export_model)

    return upstream_output


@ConfigurableStage(
    "export_image",
    Configuration(
        image_name=ConfigParam(
            str, "spectral_cube", "Output path of the spectral cube"
        ),
        export_format=ConfigParam(
            str, "fits", "Data format for the image. Allowed values: fits|zarr"
        ),
    ),
)
def export_image(upstream_output, image_name, export_format, _output_dir_):
    """
    Export the generated cube image in FITS format.

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        image_name: str
            Output file name
        export_format: str
            Data format for the image. Allowed values: fits|zarr
        _output_dir_: str
            Output directory created for the run

    Returns
    -------
        upstream_output
    """

    cube = upstream_output["image_cube"]
    output_path = os.path.join(_output_dir_, image_name)

    if export_format == "fits":
        export_task = export_to_fits(cube, output_path)
    elif export_format == "zarr":
        export_task = cube.to_zarr(store=f"{output_path}.zarr", compute=False)
    else:
        raise ValueError(f"Unsupported format: {export_format}")

    upstream_output.add_compute_tasks(export_task)

    return upstream_output
