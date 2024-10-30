from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stages.predict import predict_stage
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


@mock.patch("ska_sdp_spectral_line_imaging.stages.predict.np")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.predict.predict_for_channels"
)
def test_should_be_able_to_distribute_predict(
    predict_for_channels_mock, numpy_mock
):

    ps = Mock(name="ps")
    model = Mock(name="model")

    upstream_output = UpstreamOutput()
    upstream_output["ps"] = ps
    upstream_output["model_image"] = model

    predict_stage.stage_definition(
        upstream_output,
        epsilon=1e-4,
        cell_size=10.0,
        export_model=False,
        psout_name="ps_out",
        _output_dir_="output_path",
    )

    predict_for_channels_mock.assert_called_once_with(ps, model, 1e-4, 10.0)
