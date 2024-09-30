# pylint: disable=no-member,import-error
from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..stubs.imaging import cube_imaging
from ..util import estimate_cell_size


@ConfigurableStage(
    "imaging",
    configuration=Configuration(
        cell_size=ConfigParam(
            float,
            None,
            description="Cell size in arcsecond"
            "If None then estimation is done based on scaling factor",
        ),
        scaling_factor=ConfigParam(
            float, 3.0, description="Scaling factor for cell size estimation"
        ),
        epsilon=ConfigParam(
            float, 1e-4, description="Expected floating point precision"
        ),
        nx=ConfigParam(int, 256, description="Image size x"),
        ny=ConfigParam(int, 256, description="Image size y"),
    ),
)
def imaging_stage(upstream_output, epsilon, cell_size, scaling_factor, nx, ny):
    """
    Creates a dirty image using ducc0.gridder

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage.
        epsilon: float
            Epsilon.
        cell_size: float
            Cell size in arcsecond.
        scaling_factor: float
            Scaling factor.
        nx: int
            Image size x.
        ny: int
            Image size y.

    Returns
    -------
        dict
    """
    ps = upstream_output["ps"]

    if cell_size is None:
        uvw = ps.UVW
        # Todo: handle units properly. eg. Hz, MHz etc.
        #  Assumption, current unit is Hz.
        ref_frequency = ps.frequency.reference_frequency["data"]
        cell_size = estimate_cell_size(
            uvw, ref_frequency, nx, ny, scaling_factor
        )

    image = cube_imaging(ps, cell_size, nx, ny, epsilon)

    return {"ps": ps, "image_cube": image}
