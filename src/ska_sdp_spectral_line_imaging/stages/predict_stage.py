import numpy as np
import xarray as xr

from ska_sdp_pipelines.framework.configurable_stage import ConfigurableStage

from ..stubs.predict import predict


@ConfigurableStage("predict_stage")
def predict_stage(pipeline_data):
    ps = pipeline_data["input_data"]
    model_image = pipeline_data["output"]

    output_xr = xr.DataArray(
        np.empty(
            (
                ps.sizes["frequency"],
                ps.sizes["polarization"],
                ps.sizes["time"],
                ps.sizes["baseline_id"],
            )
        ),
        dims=["frequency", "polarization", "time", "baseline_id"],
    ).chunk(ps.chunksizes)

    return xr.map_blocks(
        predict, ps, template=output_xr, kwargs=dict(model_image=model_image)
    )
