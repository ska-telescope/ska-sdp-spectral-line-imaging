import numpy as np
import xarray as xr

from ska_sdp_spectral_line_imaging.stages.predict import predict_stage


def test_should_run_predict_stage(result_msv4):
    image_sz = 64
    model = xr.DataArray(
        np.zeros(image_sz * image_sz).reshape((image_sz, image_sz)),
        dims=["y", "x"],
    )

    stage_result = predict_stage.stage_definition(
        {"ps": result_msv4, "model_image": model}, epsilon=1e-4, cell_size=15.0
    )

    assert stage_result["ps"].VISIBILITY_MODEL.shape == (8, 1, 8, 21)


def test_should_run_dask_distributed(result_msv4):
    result_msv4.chunk(dict(frequency=2))

    image_sz = 64
    model = xr.DataArray(
        np.zeros(image_sz * image_sz).reshape((image_sz, image_sz)),
        dims=["y", "x"],
    )

    stage_result = predict_stage.stage_definition(
        {
            "ps": result_msv4.chunk(dict(frequency=2)),
            "model_image": model,
        },
        epsilon=1e-4,
        cell_size=15.0,
    )

    assert stage_result["ps"].VISIBILITY_MODEL.shape == (8, 1, 8, 21)
