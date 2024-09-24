# pylint: disable=no-member,import-error
import astropy.io.fits as fits
import numpy as np
import xarray as xr
from ska_sdp_datamodels.science_data_model.polarisation_functions import (
    convert_pol_frame,
)
from ska_sdp_datamodels.science_data_model.polarisation_model import (
    PolarisationFrame,
)

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

    ps = upstream_output["ps"]
    images = []

    for pol in pols:
        with fits.open(f"{image_name}-{pol}-image.fits") as f:
            images.append(f[0].data.squeeze())

    image_stack = xr.DataArray(
        np.stack(images), dims=["polarization", "ra", "dec"]
    )

    return {"ps": ps, "model_image": image_stack}


@ConfigurableStage(
    "vis_stokes_conversion",
    configuration=Configuration(
        ipf=ConfigParam(str, "linear"),
        opf=ConfigParam(str, "stokesIQUV"),
    ),
)
def vis_stokes_conversion(upstream_output, ipf, opf):
    """
    Visibility to stokes conversion

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        ipf: str
            Input polarization frame
        opf: str
            Output polarization frame

    Returns
    -------
        dict
    """

    ps = upstream_output["ps"]

    converted_vis = xr.apply_ufunc(
        convert_pol_frame,
        ps.VISIBILITY,
        kwargs=dict(
            ipf=PolarisationFrame(ipf),
            opf=PolarisationFrame(opf),
            polaxis=3,
        ),
        dask="allowed",
    )

    return {"ps": ps.assign(dict(VISIBILITY=converted_vis))}


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

    ps = upstream_output["ps"]

    return {
        "ps": ps.assign({"VISIBILITY": ps.VISIBILITY - ps.VISIBILITY_MODEL})
    }
