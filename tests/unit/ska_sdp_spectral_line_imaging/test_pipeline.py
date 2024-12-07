import mock

from ska_sdp_spectral_line_imaging.pipeline import diagnose, scheduler


@mock.patch(
    "ska_sdp_spectral_line_imaging.pipeline.Path",
    side_effect=["pipeline_output_path", "timestamped_output_path"],
)
@mock.patch("ska_sdp_spectral_line_imaging.pipeline.SpectralLineDiagnoser")
@mock.patch(
    "ska_sdp_spectral_line_imaging.pipeline.create_output_dir",
    return_value="timestamped_output_dir",
)
def test_should_perform_diagnostics(
    create_dir_mock,
    spectral_line_diagnose_mock,
    path_mock,
):
    spectral_line_diagnose_mock.return_value = spectral_line_diagnose_mock

    diagnose(
        input="pipeline_output_dir",
        channel=1,
        dask_scheduler="dask-scheduler-ip",
        output="output_dir",
    )

    create_dir_mock.assert_called_once_with("output_dir", "pipeline-qa")
    path_mock.assert_has_calls(
        [mock.call("pipeline_output_dir"), mock.call("timestamped_output_dir")]
    )
    spectral_line_diagnose_mock.assert_called_once_with(
        "pipeline_output_path",
        "timestamped_output_path",
        1,
        "dask-scheduler-ip",
        scheduler=scheduler,
    )
    spectral_line_diagnose_mock.diagnose.assert_called_once()


@mock.patch(
    "ska_sdp_spectral_line_imaging.pipeline.Path",
    side_effect=["pipeline_output_path", "timestamped_output_path"],
)
@mock.patch("ska_sdp_spectral_line_imaging.pipeline.SpectralLineDiagnoser")
@mock.patch(
    "ska_sdp_spectral_line_imaging.pipeline.create_output_dir",
    return_value="timestamped_output_dir",
)
def test_should_perform_diagnostics_with_default_kwargs(
    create_dir_mock, spectral_line_diagnose_mock, path_mock
):
    spectral_line_diagnose_mock.return_value = spectral_line_diagnose_mock

    diagnose(input="pipeline_output", channel=10)

    create_dir_mock.assert_called_once_with("./diagnosis", "pipeline-qa")
    path_mock.assert_has_calls(
        [mock.call("pipeline_output"), mock.call("timestamped_output_dir")]
    )
    spectral_line_diagnose_mock.assert_called_once_with(
        "pipeline_output_path",
        "timestamped_output_path",
        10,
        None,
        scheduler=scheduler,
    )
