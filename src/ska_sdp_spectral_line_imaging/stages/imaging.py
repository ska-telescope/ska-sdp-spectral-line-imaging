# pylint: disable=no-member,import-error
import numpy as np

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..stubs.imaging import cube_imaging
from ..util import estimate_cell_size, estimate_image_size


@ConfigurableStage(
    "imaging",
    configuration=Configuration(
        cell_size=ConfigParam(
            float,
            None,
            description="Cell size in arcsecond."
            " If None then estimation is done based on scaling factor.",
        ),
        scaling_factor=ConfigParam(
            float, 3.0, description="Scaling factor for cell size estimation"
        ),
        epsilon=ConfigParam(
            float, 1e-4, description="Expected floating point precision"
        ),
        image_size=ConfigParam(
            int,
            None,
            description="Dimension of the image."
            " If None, then estimation is done based on cell size.",
        ),
    ),
)
def imaging_stage(
    upstream_output, epsilon, cell_size, scaling_factor, image_size
):
    """
    Creates a dirty image using ducc0.gridder.
    Generated image is a square with length "image_size" pixels.
    Each pixel of the image is a square of length "cell_size" arcseconds.

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage.
        epsilon: float
            Epsilon.
        cell_size: float
            Cell size in arcsecond.
            If None then estimation is done based on scaling factor.
        scaling_factor: float
            Scaling factor.
        image_size: int
            Dimension of the image.
            If None, then estimation is done based on cell size.

    Returns
    -------
        dict
    """
    ps = upstream_output["ps"]

    if cell_size is None:
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

        cell_size = np.minimum(u_cell_size, v_cell_size)

    if image_size is None:
        maximum_wavelength = 3.0e8 / ps.frequency.min()
        antenna_diameter = ps.antenna_xds.DISH_DIAMETER.min()

        image_size = estimate_image_size(
            maximum_wavelength, antenna_diameter, cell_size
        )

    image = cube_imaging(ps, cell_size, image_size, image_size, epsilon)

    return {"ps": ps, "image_cube": image}
