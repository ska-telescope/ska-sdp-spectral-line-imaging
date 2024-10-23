# pylint: disable=no-member,import-error
import logging
import os

import astropy.io.fits as fits
import numpy as np
import xarray as xr
from ska_sdp_func_python.xradio.visibility.operations import (
    subtract_visibility,
)
from ska_sdp_func_python.xradio.visibility.polarization import (
    convert_polarization,
)

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..upstream_output import UpstreamOutput

logger = logging.getLogger()


@ConfigurableStage(
    "read_model",
    configuration=Configuration(
        image=ConfigParam(
            str,
            "/path/to/wsclean-%s-image.fits",
            description="""
            Path to the image file. The value must have a
            `%s` placeholder to fill-in polarization values.

            The polarization values are taken from the polarization
            coordinate present in the processing set in upstream_output.

            For example, if polarization coordinates are ['I', 'Q'],
            and `image` param is `/data/wsclean-%s-image.fits`, then the
            read_model stage will try to read
            `/data/wsclean-I-image.fits` and
            `/data/wsclean-Q-image.fits` images.

            Please refer
            `README <README.html#regarding-the-model-visibilities>`_
            to understand the requirements of the model image.
            """,
        ),
        image_type=ConfigParam(
            str,
            "continuum",
            description="Type of the input images. Available options are "
            "'spectral' or 'continuum'",
        ),
    ),
)
def read_model(
    upstream_output: UpstreamOutput,
    image: str,
    image_type: str,
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
            The polarization values are taken from the polarization
            coordinate present in the processing set in upstream_output.

            For example, if polarization coordinates are `['I', 'Q']`,
            and `image` is `/data/wsclean-%s-image.fits`, then the
            **read_model** stage will try to read
            `/data/wsclean-I-image.fits` and
            `/data/wsclean-Q-image.fits` images.

            If the corresponding image is not available in filesystem,
            this stage will raise an exception.

        image_type: str
            Whether all the images being read are "continuum"
            or "spectral"

    Returns
    -------
        UpstreamOutput

    Raises
    ------
        FileNotFoundError
            If a FITS file for a model image is not found

        AttributeError
            If the image_type parameter is invalid
    """
    # TODO: Remove this check once piper can handle enum config params.
    if image_type not in ["spectral", "continuum"]:
        raise AttributeError("image_type must be spectral or continuum")

    ps = upstream_output.ps
    pols = ps.polarization.values

    for pol in pols:
        image_path = image % pol
        if not os.path.exists(image_path):
            raise FileNotFoundError(
                f"FITS image {image_path} corresponding to "
                f"polarization {pol} not found."
            )

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
        output_polarizations=ConfigParam(
            list,
            ["I", "Q"],
            description="List of desired polarization codes, in the order "
            "they will appear in the output dataset polarization axis",
        ),
    ),
)
def vis_stokes_conversion(upstream_output, output_polarizations):
    """
    Visibility to stokes conversion

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        output_polarizations: list
            List of desired polarization codes, in the order they will appear
            in the output dataset polarization axis

    Returns
    -------
        dict
    """
    ps = upstream_output.ps

    upstream_output["ps"] = convert_polarization(ps, output_polarizations)

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
        upstream_output: UpstreamOutput
            Output from the upstream stage
        report_peak_channel: bool
            Report channel with peak emission/absorption

    Returns
    -------
        UpstreamOutput
    """

    ps = upstream_output.ps

    model = ps.assign({"VISIBILITY": ps.VISIBILITY_MODEL})
    cont_sub_ps = subtract_visibility(ps, model)
    # TODO: This has to be in ska-sdp-func-python's function
    cont_sub_ps = cont_sub_ps.assign(
        {
            "VISIBILITY": cont_sub_ps.VISIBILITY.assign_attrs(
                ps.VISIBILITY.attrs
            )
        }
    )
    upstream_output["ps"] = cont_sub_ps

    if report_peak_channel:
        # TODO: REMOVE .values once log is dask compatible.
        peak_channel = (
            np.abs(cont_sub_ps.VISIBILITY)
            .max(dim=["time", "baseline_id", "polarization"])
            .idxmax()
            .values
        )

        unit = cont_sub_ps.frequency.units[0]

        logger.warning(
            "Eager computation for peak done. This may slow down the"
            " further computations due to excesive memory usage."
        )

        logger.info(f"Peak visibility Channel: {peak_channel} {unit}")

    return upstream_output
