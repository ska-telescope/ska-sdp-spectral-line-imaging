# pylint: disable=no-member,import-error
import logging
import os

import numpy as np

from ska_sdp_piper.piper.configurations import (
    ConfigParam,
    Configuration,
    NestedConfigParam,
)
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..constants import SPEED_OF_LIGHT
from ..data_procs.imaging import clean_cube
from ..util import (
    estimate_cell_size_in_arcsec,
    estimate_image_size,
    export_image_as,
    get_polarization_frame_from_observation,
    get_wcs_from_observation,
)

logger = logging.getLogger()


def get_cell_size_from_obs(observation, scaling_factor):
    """
    A helper function which reads UVW and other metadata from
    xradio observation dataset,
    and estimates cell size to be used for imaging.

    The function is dask compatible, i.e. input dask arrays are
    not eagerly computed. Consumer of this function must call `compute()`
    on the returned object to get the actual values.

    Parameters
    ----------
        observation: xarray.Dataset
            Xradio observation
        scaling_factor: float
            Scaling factor for estimation of cell size

    Returns
    -------
        xarray.Dataarray
            Dataarray which wraps a dask array of size 1, representing
            cell size value.
    """
    umax, vmax, _ = np.abs(observation.UVW).max(dim=["time", "baseline_id"])
    # TODO: handle units properly. eg. Hz, MHz etc.
    #  Assumption, current unit is Hz.
    maximum_frequency = observation.frequency.max()
    minimum_wavelength = SPEED_OF_LIGHT / maximum_frequency

    # Taking maximum of u and v baselines, rounded
    max_baseline = np.maximum(umax, vmax).round(2)

    return estimate_cell_size_in_arcsec(
        max_baseline, minimum_wavelength, scaling_factor
    )


def get_image_size_from_obs(observation, cell_size):
    """
    A helper function which reads antenna information and other metadata from
    xradio observation dataset,
    and estimates image size to be used for imaging.

    The function is dask compatible, i.e. input dask arrays are
    not eagerly computed. Consumer of this function must call `compute()`
    on the returned object to get the actual values.

    Parameters
    ----------
        observation: xarray.Dataset
            Xradio observation
        cell_size: float
            Cell size in arcsecond.

    Returns
    -------
        xarray.Dataarray
            Dataarray which wraps a dask array of size 1, representing
            image size value.
    """
    maximum_wavelength = SPEED_OF_LIGHT / observation.frequency.min()
    # rounded to 2 decimals
    min_antenna_diameter = observation.antenna_xds.DISH_DIAMETER.min().round(2)

    return estimate_image_size(
        maximum_wavelength, min_antenna_diameter, cell_size
    )


@ConfigurableStage(
    "imaging",
    configuration=Configuration(
        gridding_params=NestedConfigParam(
            "Gridding Parameters",
            cell_size=ConfigParam(
                float,
                None,
                description="Cell Size for gridding in arcseconds. "
                "Will be calculated if None.",
            ),
            scaling_factor=ConfigParam(
                float, 3.0, description="Scalling parameter for gridding"
            ),
            epsilon=ConfigParam(float, 1e-4, description="Epsilon"),
            image_size=ConfigParam(
                int,
                256,
                description="Image Size for gridding."
                " Will be calculated if None",
            ),
        ),
        deconvolution_params=NestedConfigParam(
            "Deconvolution parameters",
            algorithm=ConfigParam(
                str,
                "generic_clean",
                nullable=False,
                description="Deconvolution algorithm.",
                allowed_values=[
                    "multiscale",
                    "iuwt",
                    "more_sane",
                    "generic_clean",
                    "hogbom",
                    "msclean",
                ],
            ),
            gain=ConfigParam(float, 0.7, description="Gain"),
            threshold=ConfigParam(float, 0.0, description="Threshold"),
            fractional_threshold=ConfigParam(
                float, 0.01, description="Fractional Threshold"
            ),
            scales=ConfigParam(
                list,
                [0, 3, 10, 30],
                description="Scalling Value for multiscale",
            ),
            niter=ConfigParam(int, 100, description="Minor cycle iterations."),
            use_radler=ConfigParam(bool, True, description="Flag for radler"),
        ),
        n_iter_major=ConfigParam(
            int,
            1,
            description="Number of major cycle iterations. "
            " If 0, only dirty image is generated.",
        ),
        psf_image_path=ConfigParam(
            str,
            None,
            description="Path to PSF FITS image. "
            "If None, the pipeline generates the psf image.",
        ),
        beam_info=ConfigParam(
            dict,
            {
                "bmaj": None,
                "bmin": None,
                "bpa": None,
            },
            description="Clean beam information, each value is in degrees",
        ),
        image_name=ConfigParam(
            str,
            "spectral_cube",
            description="Output path of the spectral cube",
            nullable=False,
        ),
        export_format=ConfigParam(
            str,
            "fits",
            description="Data format for the image. Allowed values: fits|zarr",
            allowed_values=["fits", "zarr"],
        ),
        export_model_image=ConfigParam(
            bool,
            False,
            description="Whether to export the model image "
            "generated as part of clean.",
        ),
        export_psf_image=ConfigParam(
            bool,
            False,
            description="Whether to export the psf image.",
        ),
        export_residual_image=ConfigParam(
            bool,
            False,
            description="Whether to export the residual image "
            "generated as part of clean.",
        ),
    ),
)
def imaging_stage(
    upstream_output,
    gridding_params,
    deconvolution_params,
    n_iter_major,
    psf_image_path,
    beam_info,
    image_name,
    export_format,
    export_model_image,
    export_psf_image,
    export_residual_image,
    _output_dir_,
):
    """
    Creates a dirty image using ducc0.gridder.

    For detailed parameter info, please refer to
    `Stage Configurations <../stage_config.html>`_

    Parameters
    ----------
        upstream_output: UpstreamOutput
            Output from the upstream stage
        gridding_params: dict
            Parameters for gridding the visibility
        deconvolution_params: dict
            Deconvolution parameters
        n_iter_major: int
            Major cycle iterations
        psf_image_path: str
            Path to PSF image
        beam_info: dict
            Clean beam e.g. {"bmaj":0.1, "bmin":0.05, "bpa":-60.0}.
            Units are deg, deg, deg.
            If any value is None, pipeline calculates beam
            information using psf image.
        image_name: str
            Prefix name of the exported image
        export_format: str
            "Data format for the image. Allowed values: fits|zarr"
        export_model_image: bool
            Whether to export model image
        export_psf_image: bool
            Whether to export psf image
        export_residual_image: bool
            Whether to export residual image
        _output_dir_: str
            Output directory created for the run

    Returns
    -------
        UpstreamOutput
    """

    ps = upstream_output.ps
    cell_size = gridding_params.get("cell_size", None)
    image_size = gridding_params.get("image_size", None)
    scaling_factor = gridding_params.get("scaling_factor", 3.0)

    output_path = os.path.join(_output_dir_, image_name)

    clean_products = {
        "restored": True,
        "dirty": True,
        "model": export_model_image,
        "psf": export_psf_image,
        "residual": export_residual_image,
    }

    if cell_size is None:
        logger.info("Estimating cell size...")
        cell_size = get_cell_size_from_obs(ps, scaling_factor)
        # computes
        cell_size = float(cell_size.compute(optimize_graph=True))
        gridding_params["cell_size"] = cell_size

    logger.info(f"Using cell size = {cell_size} arcseconds")

    if image_size is None:
        logger.info("Estimating image size...")
        image_size = get_image_size_from_obs(ps, cell_size)
        # computes
        image_size = float(image_size.compute(optimize_graph=True))
        gridding_params["image_size"] = image_size

    logger.info(f"Using image size = {image_size} pixels")

    gridding_params["nx"] = gridding_params["ny"] = image_size

    polarization_frame = get_polarization_frame_from_observation(ps)
    wcs = get_wcs_from_observation(
        ps, cell_size, gridding_params["nx"], gridding_params["ny"]
    )

    imaging_products = clean_cube(
        ps,
        psf_image_path,
        n_iter_major,
        gridding_params,
        deconvolution_params,
        polarization_frame,
        wcs,
        beam_info,
    )

    upstream_output.add_compute_tasks(
        *[
            export_image_as(
                imaging_products[artefact_type],
                f"{output_path}.{artefact_type}",
                export_format,
            )
            for artefact_type in imaging_products
            if clean_products[artefact_type]
        ]
    )

    return upstream_output
