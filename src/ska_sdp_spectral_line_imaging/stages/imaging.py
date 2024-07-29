# pylint: disable=no-member,import-error
import astropy.units as au
import numpy as np
import xarray as xr

from ska_sdp_pipelines.framework.configurable_stage import ConfigurableStage
from ska_sdp_pipelines.framework.configuration import (
    ConfigParam,
    Configuration,
)
from ska_sdp_spectral_line_imaging.stubs.imaging import cube_imaging


@ConfigurableStage(
    "imaging",
    configuration=Configuration(
        epsilon=ConfigParam(float, 1e-4),
        cell_size=ConfigParam(
            float, 15.0, description="Cell size in arcsecond"
        ),
        nx=ConfigParam(int, 256, description="Image size x"),
        ny=ConfigParam(int, 256, description="Image size y"),
    ),
)
def imaging_stage(upstream_output, epsilon, cell_size, nx, ny):
    """
    Creates a dirty image using ducc0.gridder
    Parameters
    ----------
        upstream_output: Any
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
        Dictionary
    """

    ps = upstream_output["ps"]

    template_core_dims = ["frequency", "polarization", "ra", "dec"]
    template_chunk_sizes = {
        k: v for k, v in ps.chunksizes.items() if k in template_core_dims
    }
    output_xr = xr.DataArray(
        np.empty(
            (
                ps.sizes["frequency"],
                ps.sizes["polarization"],
                nx,
                ny,
            )
        ),
        dims=template_core_dims,
    ).chunk(template_chunk_sizes)

    cell_size_radian = (cell_size * au.arcsecond).to(au.rad).value

    image_cube = xr.map_blocks(
        cube_imaging,
        ps,
        template=output_xr,
        kwargs=dict(
            nx=nx,
            ny=ny,
            epsilon=epsilon,
            cell_size=cell_size_radian,
        ),
    )

    return {"ps": ps, "cubes": image_cube}
