# pylint: disable=no-member,import-error
import logging
from typing import List

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
from ..upstream_output import UpstreamOutput

logger = logging.getLogger()


@ConfigurableStage(
    "read_model",
    configuration=Configuration(
        image=ConfigParam(
            str,
            "/path/to/wsclean-%s-image.fits",
            description="Path to the image file. The value must have a "
            "`%s` placeholder to fill-in polarization values. Please refer "
            "`README <README.html#regarding-the-model-visibilities>`_ "
            "to understand the requirements of the model image.",
        ),
        image_type=ConfigParam(
            str,
            "continuum",
            description="Type of the input images. Available options are "
            "'spectral' or 'continuum'",
        ),
        pols=ConfigParam(
            list, ["I", "Q"], "Polarizations of the model images"
        ),
    ),
)
def read_model(
    upstream_output: UpstreamOutput,
    image: str,
    image_type: str,
    pols: List[str],
) -> UpstreamOutput:
    """
    Read model image(s) from FITS file(s).
    Supports reading from continuum or spectral FITS images.

    Please refer `README <../README.html#regarding-the-model-visibilities>`_
    to understand the requirements of the model image.

    Parameters
    ----------
        upstream_output: UpstreamOutput
            Output from the upstream stage

        image: str
            Path to the image file. The path must have a
            `%s` placeholder to fill-in polarization values at runtime.

            For example, if `pols` is `['I', 'Q']`, and `image` is
            `/data/wsclean-%s-image.fits`, then the **read_model** stage
            will try to read `/data/wsclean-I-image.fits` and
            `/data/wsclean-Q-image.fits` images.

            For each polarization, a seperate FITS image should be available.

        image_type: str
            Whether all the images being read are "continuum"
            or "spectral"

        pols: list(str)
            A list of all the polarizations for which an image is available.

    Returns
    -------
        UpstreamOutput
    """
    # TODO: Remove this check once piper can handle enum config params.
    if image_type not in ["spectral", "continuum"]:
        raise AttributeError("image_type must be spectral or continuum")

    ps = upstream_output.ps
    images = []

    # TODO: Not dask compatible, loaded into memory by master / dask client
    for pol in pols:
        image_path = image % pol
        with fits.open(image_path) as f:
            images.append(f[0].data.squeeze())

    if image_type == "spectral":
        # Dims are assigned as per ska-data-models Image class
        # Only the "polarization" is different
        dims = ["polarization", "frequency", "y", "x"]
        chunks = {k: v for k, v in ps.chunksizes.items() if k in dims}
        model_image = xr.DataArray(
            np.stack(images, axis=0),
            dims=dims,
            coords={
                # TODO: Frequency range should be read from WCS
                # For now, copying frequency from ps
                "frequency": ps.frequency,
                "polarization": pols,
            },
        ).chunk(chunks)
    else:  # "continuum"
        dims = ["polarization", "y", "x"]
        chunks = {k: v for k, v in ps.chunksizes.items() if k in dims}
        model_image = xr.DataArray(
            np.stack(images, axis=0),
            dims=dims,
            coords={
                "polarization": pols,
            },
        ).chunk(chunks)

    upstream_output["model_image"] = model_image

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
