import logging
import os

from astropy.io import fits

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage


@ConfigurableStage(
    "export_residual",
    Configuration(
        psout_name=ConfigParam(str, "vis_residual"),
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
    output_path = os.path.join(_output_dir_, psout_name)
    ps.VISIBILITY.attrs.clear()
    ps.VISIBILITY.to_zarr(store=f"{output_path}.zarr")
    return upstream_output


@ConfigurableStage(
    "export_model",
    Configuration(
        psout_name=ConfigParam(str, "vis_model"),
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
    ps = upstream_output["ps"]
    output_path = os.path.join(_output_dir_, psout_name)
    ps.VISIBILITY_MODEL.attrs.clear()
    ps.VISIBILITY_MODEL.to_zarr(store=f"{output_path}.zarr")
    return upstream_output


@ConfigurableStage(
    "export_image",
    Configuration(
        image_name=ConfigParam(str, "spectral_cube"),
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

    cube = upstream_output["image_cube"]
    output_path = os.path.join(_output_dir_, image_name)
    logger = logging.getLogger()

    try:
        new_hdu = fits.PrimaryHDU(
            data=cube.pixels, header=cube.image_acc.wcs.to_header()
        )
        new_hdu.writeto(f"{output_path}.fits")

    except Exception as ex:
        logger.exception(ex)
        logger.info(
            "Exporting to FITS failed. "
            f"Writing image in zarr format to path {output_path}.zarr"
        )
        cube.to_zarr(store=f"{output_path}.zarr")

    return upstream_output
