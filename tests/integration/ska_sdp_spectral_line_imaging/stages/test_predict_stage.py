import numpy as np
import xarray as xr

from ska_sdp_spectral_line_imaging.stages.predict import predict_stage
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


def test_should_run_predict_stage(result_msv4):
    image_sz = 64
    model = xr.DataArray(
        np.zeros(image_sz * image_sz).reshape((image_sz, image_sz)),
        dims=["y", "x"],
    )

    upstream_output = UpstreamOutput()
    upstream_output["ps"] = result_msv4
    upstream_output["model_image"] = model

    stage_result = predict_stage.stage_definition(
        upstream_output,
        epsilon=1e-4,
        cell_size=15.0,
        export_model=False,
        psout_name="ps_out",
        _output_dir_="out_dir",
    )

    assert stage_result["ps"].VISIBILITY_MODEL.shape == (8, 1, 8, 21)


def test_should_run_dask_distributed(result_msv4):
    result_msv4.chunk(dict(frequency=2))

    image_sz = 64
    model = xr.DataArray(
        np.random.rand(1, 8, image_sz, image_sz),
        dims=["polarization", "frequency", "y", "x"],
    ).chunk(dict(frequency=2))

    upstream_output = UpstreamOutput()
    upstream_output["ps"] = result_msv4.chunk(dict(frequency=2))
    upstream_output["model_image"] = model

    stage_result = predict_stage.stage_definition(
        upstream_output,
        epsilon=1e-4,
        cell_size=15.0,
        export_model=False,
        psout_name="ps_out",
        _output_dir_="out_dir",
    )

    assert stage_result["ps"].VISIBILITY_MODEL.shape == (8, 1, 8, 21)
