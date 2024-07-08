import astropy.units as au
import numpy as np
import xarray as xr

from ska_sdp_pipelines.framework.configurable_stage import ConfigurableStage
from ska_sdp_pipelines.framework.configuration import (
    ConfigParam,
    Configuration,
)

from ..stubs.predict import predict


@ConfigurableStage(
    "predict_stage",
    configuration=Configuration(
        epsilon=ConfigParam(float, 1e-4),
        cell_size=ConfigParam(
            float, 15.0, description="Cell size in arcsecond"
        ),
    ),
)
def predict_stage(pipeline_data, epsilon=None, cell_size=None):
    ps = pipeline_data["input_data"]
    model_image = pipeline_data["output"]

    template_core_dims = ["frequency", "polarization", "time", "baseline_id"]
    template_chunk_sizes = {
        k: v for k, v in ps.chunksizes.items() if k in template_core_dims
    }
    output_xr = xr.DataArray(
        np.empty(
            (
                ps.sizes["frequency"],
                ps.sizes["polarization"],
                ps.sizes["time"],
                ps.sizes["baseline_id"],
            )
        ),
        dims=template_core_dims,
    ).chunk(template_chunk_sizes)

    cell_size_radian = (cell_size * au.arcsecond).to(au.rad).value

    return xr.map_blocks(
        predict,
        ps,
        template=output_xr,
        kwargs=dict(
            model_image=model_image,
            epsilon=epsilon,
            cell_size=cell_size_radian,
        ),
    )
