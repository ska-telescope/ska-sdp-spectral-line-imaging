import numpy as np
import xarray as xr

from ska_sdp_spectral_line_imaging.stages.predict_stage import predict_stage


def test_should_run_predict_stage(result_msv4):
    image_sz = 64
    model = xr.DataArray(
        np.zeros(image_sz * image_sz).reshape((image_sz, image_sz)),
        dims=["ra", "dec"],
    )

    pipeline_data = {"input_data": result_msv4, "output": model}

    vis = predict_stage(pipeline_data, epsilon=1e-4, cell_size=15.0)

    assert vis.shape == (8, 1, 8, 21)


def test_should_run_dask_distributed(result_msv4):
    result_msv4.chunk(dict(frequency=2))

    image_sz = 64
    model = xr.DataArray(
        np.zeros(image_sz * image_sz).reshape((image_sz, image_sz)),
        dims=["ra", "dec"],
    )

    pipeline_data = {
        "input_data": result_msv4.chunk(dict(frequency=2)),
        "output": model,
    }

    vis = predict_stage(pipeline_data, epsilon=1e-4, cell_size=15.0)

    assert vis.shape == (8, 1, 8, 21)
