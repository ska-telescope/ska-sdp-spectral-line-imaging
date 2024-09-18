# pylint: disable=no-member,import-error
from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..stubs.imaging import cube_imaging


@ConfigurableStage(
    "imaging",
    configuration=Configuration(
        cell_size=ConfigParam(
            float, 60.0, description="Cell size in arcsecond"
        ),
        epsilon=ConfigParam(
            float, 1e-4, description="Expected floating point precision"
        ),
        nx=ConfigParam(int, 256, description="Image size x"),
        ny=ConfigParam(int, 256, description="Image size y"),
    ),
)
def imaging_stage(upstream_output, epsilon, cell_size, nx, ny, _input_data_):
    """
    Creates a dirty image using ducc0.gridder

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        epsilon: float
            Epsilon
        cell_size: float
            Cell size in arcsecond
        nx: int
            Image size x
        ny: int
            Image size y

    Returns
    -------
        dict
    """

    ps = upstream_output["ps"]

    image = cube_imaging(ps, cell_size, nx, ny, epsilon)

    return {"ps": ps, "image_cube": image}
