# pylint: disable=no-member,import-error
import numpy as np
from ska_sdp_datamodels.image import import_image_from_fits

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..stubs.imaging import clean_cube
from ..util import estimate_cell_size, estimate_image_size


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
                "prefix": "",
                "window_shape": None,
                "mask": None,
                "algorithm": "hogbom",
                "gain": 0.7,
                "threshold": 0.0,
                "fractional_threshold": 0.01,
                "scales": [0, 3, 10, 30],
                "nmoment": 3,
                "findpeak": "RASCIL",
                "niter": 100,
            },
            description="Deconvolution parameters",
        ),
        n_iter_major=ConfigParam(int, 0, description="Major cycle iterations"),
        psf_image_path=ConfigParam(str, None, description="Path to PSF image"),
    ),
)
def imaging_stage(
    upstream_output,
    gridding_params,
    deconvolution_params,
    n_iter_major,
    psf_image_path,
):
    """
    Creates a dirty image using ducc0.gridder.
    Generated image is a square with length "image_size" pixels.
    Each pixel of the image is a square of length "cell_size" arcseconds.

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        gridding_params: dict
            Parameters for gridding the visibility
        deconvolution_params: dict
            Deconvolution parameters
        n_iter_major: int
            Major cycle iterations
        psf_image_path: str
            Path to PSF image
    Returns
    -------
        dict
    """
    ps = upstream_output["ps"]
    cell_size = gridding_params.get("cell_size", None)
    image_size = gridding_params.get("image_size", None)
    if cell_size is None:
        scaling_factor = gridding_params.get("scaling_factor", 3.0)
        umax, vmax, _ = np.abs(ps.UVW).max(dim=["time", "baseline_id"])
        # TODO: handle units properly. eg. Hz, MHz etc.
        #  Assumption, current unit is Hz.
        maximum_frequency = ps.frequency.max()
        minimum_wavelength = 3.0e8 / maximum_frequency

        u_cell_size = estimate_cell_size(
            umax, minimum_wavelength, scaling_factor
        )
        v_cell_size = estimate_cell_size(
            vmax, minimum_wavelength, scaling_factor
        )

        gridding_params["cell_size"] = np.minimum(u_cell_size, v_cell_size)

    if image_size is None:
        maximum_wavelength = 3.0e8 / ps.frequency.min()
        antenna_diameter = ps.antenna_xds.DISH_DIAMETER.min()

        image_size = estimate_image_size(
            maximum_wavelength, antenna_diameter, cell_size
        )

    gridding_params["nx"] = gridding_params["ny"] = image_size

    if psf_image_path is None:
        psf_image = []
    else:
        psf_image = import_image_from_fits(psf_image_path, fixpol=True)

    image = clean_cube(
        ps,
        psf_image,
        n_iter_major,
        gridding_params,
        deconvolution_params,
    )

    return {"ps": upstream_output["ps"], "image_cube": image}
