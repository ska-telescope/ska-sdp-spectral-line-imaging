# pylint: disable=no-member,import-error
import astropy.io.fits as fits
import numpy as np
import xarray as xr

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage


@ConfigurableStage(
    "read_model",
    configuration=Configuration(
        image_name=ConfigParam(str, "wsclean"),
        pols=ConfigParam(list, ["I", "Q", "U", "V"]),
    ),
)
def read_model(upstream_output, image_name, pols):
    """
    Read model from the image

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        image_name: str
            Name of the image to be read
        pos: list(str)
            Polarizations to be included

    Returns
    -------
        dict
    """

    images = []

    for pol in pols:
        with fits.open(f"{image_name}-{pol}-image.fits") as f:
            images.append(f[0].data.squeeze())

    image_stack = xr.DataArray(
        np.stack(images), dims=["polarization", "ra", "dec"]
    )

    return image_stack


@ConfigurableStage("continuum_subtraction")
def cont_sub(upstream_output):
    """
    Perform continuum subtraction

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage

    Returns
    -------
        dict
    """

    ps = upstream_output["predict_stage"]

    return ps.assign({"VISIBILITY": ps.VISIBILITY - ps.VISIBILITY_MODEL})
