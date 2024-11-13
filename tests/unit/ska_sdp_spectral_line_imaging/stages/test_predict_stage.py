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
    ps.polarization = Mock(name="polarization")
    ps.polarization.values = ["RR", "LL"]

    ps.assign = Mock(name="assign", return_value=ps)

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


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.predict.os.path.join",
    return_value="JOINED_PATH",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.predict.export_to_zarr",
    return_value="DELAYED_EXPORT",
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.predict.np")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.predict.predict_for_channels"
)
def test_should_export_model(
    predict_for_channels_mock, numpy_mock, export_zarr_mock, os_join_mock
):

    ps = Mock(name="ps")
    ps.polarization = Mock(name="polarization")
    ps.polarization.values = ["RR", "LL"]

    ps.assign = Mock(name="assign", return_value=ps)

    model = Mock(name="model")

    upstream_output = UpstreamOutput()
    upstream_output["ps"] = ps
    upstream_output["model_image"] = model

    output = predict_stage.stage_definition(
        upstream_output,
        epsilon=1e-4,
        cell_size=10.0,
        export_model=True,
        psout_name="ps_out",
        _output_dir_="output_path",
    )

    os_join_mock.assert_called_once_with("output_path", "ps_out")
    export_zarr_mock.assert_called_once_with(
        ps.VISIBILITY_MODEL, "JOINED_PATH", clear_attrs=True
    )

    assert "DELAYED_EXPORT" in output.compute_tasks
