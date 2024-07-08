from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stages.predict_stage import predict_stage


@mock.patch("ska_sdp_spectral_line_imaging.stages.predict_stage.predict")
@mock.patch("ska_sdp_spectral_line_imaging.stages.predict_stage.xr.DataArray")
@mock.patch("ska_sdp_spectral_line_imaging.stages.predict_stage.xr.map_blocks")
def test_should_be_able_to_distribute_predict(
    map_block_mock, dataarray_mock, predict_mock
):

    dataarray_mock.return_value = dataarray_mock
    dataarray_mock.chunk.return_value = "CHUNKED_DATA"
    mock_chunks = dict(frequency=32, polarization=1, time=1, baseline_id=1)
    ps = Mock(name="ps")
    ps.chunksizes = mock_chunks
    ps.sizes = mock_chunks
    model = Mock(name="model")

    pipeline_data = {"input_data": ps, "output": model}

    predict_stage(pipeline_data)

    map_block_mock.assert_called_once_with(
        predict_mock,
        ps,
        template="CHUNKED_DATA",
        kwargs=dict(model_image=model),
    )
