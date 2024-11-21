# pylint: disable=no-member,import-error
import logging
import os

import dask
import dask.array
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
from ska_sdp_piper.piper.utils import delayed_log

from ..data_procs.model import (
    apply_power_law_scaling,
    fit_polynomial_on_visibility,
    report_peak_visibility,
)
from ..upstream_output import UpstreamOutput
from ..util import export_to_zarr, get_dataarray_from_fits

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
            nullable=False,
        ),
        do_power_law_scaling=ConfigParam(
            bool,
            False,
            description="""
            Whether to perform power law scaling to scale
            model image across channels. Only applicable for
            continuum images.
            """,
        ),
        spectral_index=ConfigParam(
            float,
            0.75,
            description="""
            Spectral index (alpha) to perform power law scaling.
            Note: The ratio of frequencies is raised to `-spectral_index`
            Please refer `read_model stage
            <api/ska_sdp_spectral_line_imaging.stages.model.html>`_ for
            information on the formula.
            """,
        ),
    ),
)
def read_model(
    upstream_output: UpstreamOutput,
    image: str,
    do_power_law_scaling: bool,
    spectral_index: float,
) -> UpstreamOutput:
    """
    Read model image(s) from FITS file(s).
    Supports reading from continuum or spectral FITS images.

    Please refer `README <../README.html#regarding-the-model-visibilities>`_
    to understand the requirements of the model image.

    If `do_power_law_scaling` is True, this function can scale model image
    across channels. This is only applicable for images with single
    frequency channel. The formula for power law scaling is as follows

    .. math::

        scaled\\_channel = current\\_channel *
            (\\frac{channel\\_frequency}
            {reference\\_frequency})^{-\\alpha}

    Where:

        - :math:`{channel\\_frequency}`: Frequency of the current channel
        - :math:`{reference\\_frequency}`: Reference frequency
        - :math:`{\\alpha}`: Spectral index

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

        do_power_law_scaling: bool
            Whether to perform power law scaling to scale
            model image across channels. Only applicable for
            continuum images.

        spectral_index: float
            Spectral index (alpha) to perform power law scaling.
            Note: The ratio of frequencies is raised to `-spectral_index`

    Returns
    -------
        UpstreamOutput
    """
    ps = upstream_output.ps
    pols = ps.polarization.values

    images = []
    for pol in pols:
        image_path = image % pol
        image_xr = get_dataarray_from_fits(image_path)
        images.append(image_xr)

    if "polarization" in images[0].dims:
        model_image = xr.concat(images, dim="polarization")
    else:
        # stack the images creating new polarization axis
        # assuming that all images have same dims and coords
        model_image_data = dask.array.stack(images, axis=0)
        model_image = xr.DataArray(
            model_image_data,
            dims=["polarization", *images[0].dims],
            name="model_image",
        )
        model_image = model_image.assign_coords(images[0].coords)
        model_image = model_image.assign_coords({"polarization": pols})

    if do_power_law_scaling:
        model_image = apply_power_law_scaling(
            model_image,
            ps.frequency.data,
            spectral_index=spectral_index,
            freq_chunks=ps.chunks["frequency"],
        )

    if "frequency" in model_image.dims and model_image.frequency.size == 1:
        # continuum image with extra dimension on frequency
        model_image = model_image.squeeze(dim="frequency", drop=True)

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
        export_residual=ConfigParam(
            bool, False, description="Export the residual visibilities"
        ),
        psout_name=ConfigParam(
            str,
            "vis_residual",
            description="Output file name prefix of residual data",
            nullable=False,
        ),
        report_poly_fit=ConfigParam(
            bool,
            False,
            description="Whether to report extent of continuum subtraction "
            "by fitting polynomial across channels",
        ),
    ),
)
def cont_sub(
    upstream_output,
    export_residual,
    psout_name,
    report_poly_fit,
    _output_dir_,
):
    """
    Perform continuum subtraction

    Parameters
    ----------
        upstream_output: UpstreamOutput
            Output from the upstream stage
        export_residual: bool
            Export the residual visibilities
        psout_name: str
            Output file name prefix of residual data

    Returns
    -------
        UpstreamOutput
    """

    ps = upstream_output.ps

    model = ps.assign({"VISIBILITY": ps.VISIBILITY_MODEL})
    cont_sub_ps = subtract_visibility(ps, model)

    if export_residual:
        output_path = os.path.join(_output_dir_, psout_name)
        upstream_output.add_compute_tasks(
            export_to_zarr(
                cont_sub_ps.VISIBILITY, output_path, clear_attrs=True
            )
        )

    # TODO: This has to be in ska-sdp-func-python's function
    cont_sub_ps = cont_sub_ps.assign(
        {
            "VISIBILITY": cont_sub_ps.VISIBILITY.assign_attrs(
                ps.VISIBILITY.attrs
            )
        }
    )
    # Sending a copy to avoid issues where attributes get cleared
    upstream_output["ps"] = cont_sub_ps.copy()

    # Report peak visibility and corresponding channel
    upstream_output.add_compute_tasks(
        report_peak_visibility(
            cont_sub_ps.VISIBILITY, cont_sub_ps.frequency.units[0]
        )
    )

    # Report extent of continuum subtraction
    if report_poly_fit:
        pols = ps.polarization.values
        valid_sets = [{"XX", "YY"}, {"RR", "LL"}]
        if not set(pols) in valid_sets:
            logger.warning("Cannot report extent of continuum subtraction.")
        else:
            cont_sub_vis = cont_sub_ps.VISIBILITY.where(
                np.logical_not(cont_sub_ps.FLAG)
            )
            fit_real = fit_polynomial_on_visibility(cont_sub_vis.real)
            fit_imag = fit_polynomial_on_visibility(cont_sub_vis.imag)
            upstream_output.add_compute_tasks(
                delayed_log(
                    logger.info,
                    "Slope of fit on real part {fit}",
                    fit=fit_real[1],
                )
            )
            upstream_output.add_compute_tasks(
                delayed_log(
                    logger.info,
                    "Slope of fit on imag part {fit}",
                    fit=fit_imag[1],
                )
            )

    return upstream_output
