from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stages.predict import predict_stage


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.predict.predict_for_channels"
)
def test_should_be_able_to_distribute_predict(predict_for_channels_mock):

    ps = Mock(name="ps")
    model = Mock(name="model")

    predict_stage.stage_definition(
        {"ps": ps, "model_image": model}, epsilon=1e-4, cell_size=10.0
    )

    predict_for_channels_mock.assert_called_once_with(ps, model, 1e-4, 10.0)
