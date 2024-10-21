# pylint: disable=no-member,import-error
import logging

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

from ..stubs.model import subtract_visibility

logger = logging.getLogger()


@ConfigurableStage(
    "read_model",
    configuration=Configuration(
        image_name=ConfigParam(
            str,
            "wsclean",
            description="Prefix path of the image(s) which contain "
            "model data. Please refer "
            "`README <README.html#regarding-the-model-visibilities>`_ "
            "to understand the pre-requisites of the pipeline.",
        ),
        pols=ConfigParam(
            list, ["I", "Q", "U", "V"], "Polarizations of the model images"
        ),
    ),
)
def read_model(upstream_output, image_name, pols):
    """
    Read model from the image.
    Please refer `README <../README.html#regarding-the-model-visibilities>`_
    to understand the supported format for model images.

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        image_name: str
            Prefix path of the image(s) which contain model data
        pos: list(str)
            Polarizations of the model image(s)

    Returns
    -------
        dict
    """

    images = []
    # TODO: Not dask compatible, loaded into memory by master / dask client
    for pol in pols:
        with fits.open(f"{image_name}-{pol}-image.fits") as f:
            images.append(f[0].data.squeeze())

    # Dims are assigned as per ska-data-models Image class
    # Only the "polarization" is different
    image_stack = xr.DataArray(
        np.stack(images), dims=["polarization", "y", "x"]
    )

    upstream_output["model_image"] = image_stack

    return upstream_output


@ConfigurableStage(
    "vis_stokes_conversion",
    configuration=Configuration(
        input_polarisation_frame=ConfigParam(
            str,
            "linear",
            description="Polarization frame of the input visibility. "
            "Supported options are: 'circular','circularnp', "
            "'linear', 'linearnp', 'stokesIQUV', 'stokesIV', "
            "'stokesIQ', 'stokesI'.",
        ),
        output_polarisation_frame=ConfigParam(
            str,
            "stokesIQUV",
            description="Polarization frame of the output visibilities. "
            "Supported options are same as output_polarisation_frame",
        ),
    ),
)
def vis_stokes_conversion(
    upstream_output, input_polarisation_frame, output_polarisation_frame
):
    """
    Visibility to stokes conversion

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        input_polarisation_frame: str
            Input polarization frame
        output_polarisation_frame: str
            Output polarization frame

    Returns
    -------
        dict
    """

    # TODO: Replace this entire function with the one present
    # in sdp-func-python as that is more accurate

    ps = upstream_output.ps

    converted_vis = xr.apply_ufunc(
        convert_pol_frame,
        ps.VISIBILITY,
        kwargs=dict(
            ipf=PolarisationFrame(input_polarisation_frame),
            opf=PolarisationFrame(output_polarisation_frame),
            polaxis=3,
        ),
        keep_attrs=True,
        dask="allowed",
    )

    converted_vis = converted_vis.assign_coords(
        {
            "polarization": np.array(
                PolarisationFrame(output_polarisation_frame).names
            )
        }
    )

    converted_vis = converted_vis.assign_attrs(ps.VISIBILITY.attrs)

    ps = ps.assign({"VISIBILITY": converted_vis})

    upstream_output["ps"] = ps

    return upstream_output


@ConfigurableStage(
    "continuum_subtraction",
    configuration=Configuration(
        report_peak_channel=ConfigParam(
            bool,
            True,
            description="Report channel with peak emission/absorption",
        ),
    ),
)
def cont_sub(upstream_output, report_peak_channel):
    """
    Perform continuum subtraction

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        report_peak_channel: bool
            Report channel with peak emission/absorption

    Returns
    -------
        dict
    """

    ps = upstream_output.ps

    model = ps.assign({"VISIBILITY": ps.VISIBILITY_MODEL})
    cont_sub_ps = subtract_visibility(ps, model)
    upstream_output["ps"] = cont_sub_ps

    if report_peak_channel:
        peak_channel = (
            np.abs(cont_sub_ps.VISIBILITY)
            .max(dim=["time", "baseline_id", "polarization"])
            .idxmax()
            .values
        )

        unit = cont_sub_ps.frequency.units[0]

        logger.info(f"Peak visibility Channel: {peak_channel} {unit}")

    return upstream_output
