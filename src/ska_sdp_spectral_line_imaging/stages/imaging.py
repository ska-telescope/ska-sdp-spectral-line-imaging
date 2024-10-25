# pylint: disable=no-member,import-error
import logging
import os

import numpy as np

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage
from ska_sdp_piper.piper.utils import delayed_log

from ..stubs.imaging import clean_cube, cube_imaging
from ..util import (
    estimate_cell_size,
    estimate_image_size,
    export_data_as,
    get_polarization,
    get_wcs,
)

logger = logging.getLogger()

# TODO: Find better place for constants
SPEED_OF_LIGHT = 299792458


@ConfigurableStage(
    "imaging",
    configuration=Configuration(
        gridding_params=ConfigParam(
            dict,
            {
                "cell_size": None,
                "scaling_factor": 3.0,
                "epsilon": 1e-4,
                "image_size": 256,
            },
            description="Gridding parameters",
        ),
        deconvolution_params=ConfigParam(
            dict,
            {
                "algorithm": "multiscale",
                "gain": 0.7,
                "threshold": 0.0,
                "fractional_threshold": 0.01,
                "scales": [0, 3, 10, 30],
                "niter": 100,
                "use_radler": True,
            },
            description="Deconvolution parameters",
        ),
        n_iter_major=ConfigParam(
            int, 0, description="Number of major cycle iterations"
        ),
        do_clean=ConfigParam(
            bool,
            False,
            description="Whether to run clean algorithm. "
            "If False, only the dirty image is generated. "
            "If True, the restored image is generated.",
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
            description="Beam information. "
            "If any value is None, "
            "pipeline calculates beam information using psf image.",
        ),
        image_name=ConfigParam(
            str, "spectral_cube", "Output path of the spectral cube"
        ),
        export_format=ConfigParam(
            str, "fits", "Data format for the image. Allowed values: fits|zarr"
        ),
        export_psf_image=ConfigParam(
            bool,
            False,
            description="Whether to export the psf image.",
        ),
        export_model_image=ConfigParam(
            bool,
            False,
            description="Whether to export the model image "
            "generated as part of clean.",
        ),
        export_residual_image=ConfigParam(
            bool,
            False,
            description="Whether to export the residual image "
            "generated as part of clean.",
        ),
        export_image=ConfigParam(
            bool,
            False,
            description="Whether to export the restored image "
            "generated as part of clean. If clean is not run then "
            "export the dirty image",
        ),
    ),
)
def imaging_stage(
    upstream_output,
    gridding_params,
    deconvolution_params,
    do_clean,
    n_iter_major,
    psf_image_path,
    beam_info,
    image_name,
    export_format,
    export_psf_image,
    export_model_image,
    export_residual_image,
    export_image,
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
        do_clean: bool
            Whether to run clean algorithm or not
        n_iter_major: int
            Major cycle iterations
        psf_image_path: str
            Path to PSF image
        beam_info: dict
            Beam information
        image_name: str
            Prefix name of the exported image
        export_format: str
            "Data format for the image. Allowed values: fits|zarr"
        export_psf_image: bool
            Whether to export psf image
        export_model_image: bool
            Whether to export model image
        export_residual_image: bool
            Whether to export residual image
        export_image: bool
            Whether to export restored/dirty image
        _output_dir_: str
            Output directory created for the run

    Returns
    -------
        UpstreamOutput
    """

    ps = upstream_output.ps
    cell_size = gridding_params.get("cell_size", None)
    image_size = gridding_params.get("image_size", None)
    output_path = os.path.join(_output_dir_, image_name)

    clean_export_flags = {
        "model": export_model_image,
        "psf": export_psf_image,
        "residual": export_residual_image,
        "restored": export_image,
    }

    if cell_size is None:
        scaling_factor = gridding_params.get("scaling_factor", 3.0)
        umax, vmax, _ = np.abs(ps.UVW).max(dim=["time", "baseline_id"])
        # TODO: handle units properly. eg. Hz, MHz etc.
        #  Assumption, current unit is Hz.
        maximum_frequency = ps.frequency.max()
        minimum_wavelength = SPEED_OF_LIGHT / maximum_frequency

        # Taking maximum of u and v baselines, rounded
        max_baseline = np.maximum(umax, vmax).round(2)
        upstream_output.add_compute_tasks(
            delayed_log(
                logger.info,
                "Estimating cell size using baseline of "
                "{max_baseline} meters",
                max_baseline=[max_baseline, float],
            )
        )

        cell_size = estimate_cell_size(
            max_baseline, minimum_wavelength, scaling_factor
        )
        gridding_params["cell_size"] = cell_size

    upstream_output.add_compute_tasks(
        delayed_log(
            logger.info,
            "Using cell size = {cell_size} arcseconds",
            cell_size=[cell_size, float],
        )
    )

    if image_size is None:
        maximum_wavelength = SPEED_OF_LIGHT / ps.frequency.min()
        antenna_diameter = ps.antenna_xds.DISH_DIAMETER.min().round(2)

        upstream_output.add_compute_tasks(
            delayed_log(
                logger.info,
                "Estimating image size using antenna diameter of "
                "{antenna_diameter} meters",
                antenna_diameter=[antenna_diameter, float],
            )
        )

        image_size = estimate_image_size(
            maximum_wavelength, antenna_diameter, cell_size
        )
        gridding_params["image_size"] = image_size

    upstream_output.add_compute_tasks(
        delayed_log(
            logger.info,
            "Using image size = {image_size} pixels",
            image_size=[image_size, int],
        )
    )

    gridding_params["nx"] = gridding_params["ny"] = image_size

    polarization_frame = get_polarization(ps)
    wcs = get_wcs(ps, cell_size, gridding_params["nx"], gridding_params["ny"])

    dirty_image = cube_imaging(
        ps,
        cell_size,
        gridding_params["nx"],
        gridding_params["ny"],
        gridding_params["epsilon"],
        wcs,
        polarization_frame,
    )

    output_image = dirty_image

    if do_clean:
        imaging_products = clean_cube(
            ps,
            psf_image_path,
            dirty_image,
            n_iter_major,
            gridding_params,
            deconvolution_params,
            polarization_frame,
            wcs,
            beam_info,
        )

        output_image = imaging_products["restored"]

        upstream_output.add_compute_tasks(
            *[
                export_data_as(
                    imaging_products[artefact_type],
                    f"{output_path}.{artefact_type}",
                    export_format,
                )
                for artefact_type, export_flag in clean_export_flags.items()
                if export_flag
            ]
        )

    elif export_image:
        upstream_output.add_compute_tasks(
            export_data_as(
                dirty_image,
                f"{output_path}.dirty",
                export_format,
            )
        )

    upstream_output["image_cube"] = output_image

    return upstream_output
