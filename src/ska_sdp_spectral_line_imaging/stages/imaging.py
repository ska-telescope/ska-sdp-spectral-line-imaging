# pylint: disable=no-member,import-error
import logging
import os

from ska_sdp_piper.piper.configurations import (
    ConfigParam,
    Configuration,
    NestedConfigParam,
)
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..data_procs.imaging import (
    clean_cube,
    get_cell_size_from_obs,
    get_image_size_from_obs,
)
from ..util import export_image_as

logger = logging.getLogger()


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
                description="""
                Deconvolution algorithm. Note that 'hogbom' and 'msclean'
                are only allowed when radler is not used.
                """,
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
    Performs clean algorithm on the visibilities present in
    processing set. Processing set is present in from the upstream_output.

    For detailed parameter info, please refer to
    "Stage Config" section in the documentation.

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

    # clean_cube function expects 'nx' and 'ny' in gridding_params
    gridding_params["nx"] = gridding_params["ny"] = image_size

    imaging_products = clean_cube(
        ps,
        psf_image_path,
        n_iter_major,
        gridding_params,
        deconvolution_params,
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
