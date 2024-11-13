# pylint: disable=no-member
import numpy as np
from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stubs.predict import (
    predict,
    predict_ducc,
    predict_for_channels,
)


@mock.patch("ska_sdp_spectral_line_imaging.stubs.predict.ducc0.wgridder")
def test_should_be_able_to_degrid_image(wgridder_mock):
    weight = Mock(spec=np.array(()), name="weight.np.array")
    weight.reshape.return_value = "RESHAPED_WEIGHT"
    flag = Mock(spec=np.array(()), name="flag.np.array")
    flag.reshape.return_value = "RESHAPED_FLAG"
    uvw = Mock(spec=np.array(()), name="uvw.np.array")
    uvw.reshape.return_value = "RESHAPED_UVW"
    freq = Mock(spec=np.array(()), name="freq.np.array")
    freq.reshape.return_value = "RESHAPED_FREQ"

    ducc_return_mock = Mock(spec=np.array(()), name="ducc_return.np.array")
    ducc_return_mock.reshape.return_value = "reshaped_model"
    wgridder_mock.dirty2ms.return_value = ducc_return_mock

    cell_size = 0.001
    nchan = 1
    ntime = 10
    nbaseline = 6
    epsilon = 1e-4

    output = predict_ducc(
        weight,
        flag,
        uvw,
        freq,
        "model_image",
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
        "model_image",
        "RESHAPED_WEIGHT",
        0.001,
        0.001,
        0,
        0,
        1e-4,
        nthreads=1,
    )
    ducc_return_mock.reshape.assert_called_once_with(10, 6)
    assert output == "reshaped_model"


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
            ["y", "x"],
        ],
        output_core_dims=[["time", "baseline_id"]],
        vectorize=True,
        keep_attrs=True,
        dask="parallelized",
        output_dtypes=[np.complex64],
        kwargs=dict(
            nchan=1,
            ntime=10,
            nbaseline=10,
            epsilon=1e-4,
            cell_size=0.01,
        ),
    )


@mock.patch("ska_sdp_spectral_line_imaging.stubs.predict.predict")
@mock.patch("ska_sdp_spectral_line_imaging.stubs.predict.np")
def test_should_be_able_to_distribute_predict(numpy_mock, predict_mock):
    numpy_mock.deg2rad.return_value = 0.5
    ps = Mock(name="ps")
    predicted_vis = Mock(name="predicted_visibility")
    predict_mock.return_value = predicted_vis
    predicted_vis.assign_coords.return_value = "predicted_visibility"
    model_image_mock = Mock(name="model_image")
    model_image_mock.astype.return_value = "model_image_converted"

    output = predict_for_channels(
        ps, model_image_mock, epsilon=1e-4, cell_size=7200
    )

    numpy_mock.deg2rad.assert_called_once_with(2)
    predict_mock.assert_called_once_with(
        ps, "model_image_converted", epsilon=1e-4, cell_size=0.5
    )

    predicted_vis.assign_coords.assert_called_once_with(ps.VISIBILITY.coords)

    assert output == "predicted_visibility"
