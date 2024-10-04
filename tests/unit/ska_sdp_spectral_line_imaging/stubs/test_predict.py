# pylint: disable=no-member
import numpy as np
from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stubs.predict import (
    predict,
    predict_ducc,
    predict_for_channels,
)


@mock.patch("ska_sdp_spectral_line_imaging.stubs.predict.ducc0.wgridder")
def test_should_be_able_to_grid_data(wgridder_mock):
    weight = Mock(spec=np.array(()), name="weight.np.array")
    weight.reshape.return_value = "RESHAPED_WEIGHT"
    flag = Mock(spec=np.array(()), name="flag.np.array")
    flag.reshape.return_value = "RESHAPED_FLAG"
    uvw = Mock(spec=np.array(()), name="uvw.np.array")
    uvw.reshape.return_value = "RESHAPED_UVW"
    freq = Mock(spec=np.array(()), name="freq.np.array")
    freq.reshape.return_value = "RESHAPED_FREQ"

    ducc_return_mock = Mock(spec=np.array(()), name="ducc_return.np.array")
    wgridder_mock.dirty2ms.return_value = ducc_return_mock

    model_image = Mock(name="model_image")
    cell_size = 0.001
    nchan = 1
    ntime = 10
    nbaseline = 6
    epsilon = 1e-4

    predict_ducc(
        weight,
        flag,
        uvw,
        freq,
        model_image,
        cell_size,
        epsilon,
        nchan,
        ntime,
        nbaseline,
    )

    uvw.reshape.assert_called_once_with(60, 3)
    weight.reshape.assert_called_once_with(60, 1)
    freq.reshape.assert_called_once_with(1)
    wgridder_mock.dirty2ms.assert_called_once_with(
        "RESHAPED_UVW",
        "RESHAPED_FREQ",
        model_image,
        "RESHAPED_WEIGHT",
        cell_size,
        cell_size,
        0,
        0,
        epsilon,
        nthreads=1,
    )
    ducc_return_mock.reshape.assert_called_once_with(10, 6)


@mock.patch("ska_sdp_spectral_line_imaging.stubs.predict.xr")
def test_should_able_to_apply_prediction_on_all_chan(xarray_mock):
    ps = Mock(name="ps")
    ps.WEIGHT = Mock(name="weights")
    ps.UVW = Mock(name="uvw")
    ps.frequency = Mock(name="frequency")
    ps.time.size = 10
    ps.baseline_id.size = 10
    model = Mock(name="model")

    predict(ps, model, epsilon=1e-4, cell_size=0.01)

    xarray_mock.apply_ufunc.assert_called_once_with(
        predict_ducc,
        ps.WEIGHT,
        ps.FLAG,
        ps.UVW,
        ps.frequency,
        model,
        input_core_dims=[
            ["time", "baseline_id"],
            ["time", "baseline_id"],
            ["time", "baseline_id", "uvw_label"],
            [],
            ["ra", "dec"],
        ],
        output_core_dims=[["time", "baseline_id"]],
        vectorize=True,
        kwargs=dict(
            nchan=1,
            ntime=10,
            nbaseline=10,
            epsilon=1e-4,
            cell_size=0.01,
        ),
    )


@mock.patch("ska_sdp_spectral_line_imaging.stubs.predict.predict")
@mock.patch("ska_sdp_spectral_line_imaging.stubs.predict.xr.DataArray")
@mock.patch("ska_sdp_spectral_line_imaging.stubs.predict.xr.map_blocks")
@mock.patch("ska_sdp_spectral_line_imaging.stubs.predict.np")
def test_should_be_able_to_distribute_predict(
    numpy_mock, map_block_mock, dataarray_mock, predict_mock
):
    dataarray_mock.return_value = dataarray_mock
    dataarray_mock.chunk.return_value = "CHUNKED_DATA"
    numpy_mock.deg2rad.return_value = 4
    mock_chunks = dict(frequency=32, polarization=1, time=1, baseline_id=1)
    ps = Mock(name="ps")
    ps.chunksizes = mock_chunks
    ps.sizes = mock_chunks
    predicted_vis = Mock(name="predicted_visibility")
    map_block_mock.return_value = predicted_vis

    predict_for_channels(ps, "model_image", epsilon=1e-4, cell_size=7200)

    numpy_mock.deg2rad.assert_called_once_with(2)
    map_block_mock.assert_called_once_with(
        predict_mock,
        ps,
        template="CHUNKED_DATA",
        kwargs=dict(model_image="model_image", epsilon=1e-4, cell_size=4),
    )
    predicted_vis.assign_coords.assert_called_once_with(ps.VISIBILITY.coords)
