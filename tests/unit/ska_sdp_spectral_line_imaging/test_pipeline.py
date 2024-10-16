from mock import Mock, mock

from ska_sdp_spectral_line_imaging.pipeline import (
    pipeline_diagnostic,
    scheduler,
)


@mock.patch(
    "ska_sdp_spectral_line_imaging.pipeline.Path",
    side_effect=["input_dir", "output_dir"],
)
@mock.patch("ska_sdp_spectral_line_imaging.pipeline.SpectralLineDiagnoser")
@mock.patch(
    "ska_sdp_spectral_line_imaging.pipeline.create_output_dir",
    return_value="output_path",
)
def test_should_perform_diagnostics(
    create_dir_mock,
    spectral_line_diagnose_mock,
    path_mock,
):
    cli_args = Mock(name="cli_args")
    cli_args.input = "input"
    cli_args.output = "output"
    cli_args.channel = 1
    cli_args.dask_scheduler = "dask-scheduler"
    spectral_line_diagnose_mock.return_value = spectral_line_diagnose_mock

    pipeline_diagnostic(cli_args)
    path_mock.assert_has_calls([mock.call("input"), mock.call("output_path")])
    create_dir_mock.assert_called_once_with("output", "pipeline-qa")
    spectral_line_diagnose_mock.assert_called_once_with(
        "input_dir",
        "output_dir",
        1,
        "dask-scheduler",
        scheduler=scheduler,
    )
    spectral_line_diagnose_mock.diagnose.assert_called_once()


@mock.patch(
    "ska_sdp_spectral_line_imaging.pipeline.Path",
    side_effect=["input_dir", "output_dir"],
)
@mock.patch("ska_sdp_spectral_line_imaging.pipeline.SpectralLineDiagnoser")
@mock.patch(
    "ska_sdp_spectral_line_imaging.pipeline.create_output_dir",
    return_value="output_path",
)
def test_should_perform_diagnostics_with_default_output(
    create_dir_mock, spectral_line_diagnose_mock, path_mock
):
    cli_args = Mock(name="cli_args")
    cli_args.input = "input"
    cli_args.output = None
    cli_args.channel = 1
    cli_args.dask_scheduler = "dask-scheduler"
    spectral_line_diagnose_mock.return_value = spectral_line_diagnose_mock

    pipeline_diagnostic(cli_args)
    path_mock.assert_has_calls([mock.call("input"), mock.call("output_path")])
    create_dir_mock.assert_called_once_with("./diagnosis", "pipeline-qa")
    spectral_line_diagnose_mock.assert_called_once_with(
        "input_dir",
        "output_dir",
        1,
        "dask-scheduler",
        scheduler=scheduler,
    )
    spectral_line_diagnose_mock.diagnose.assert_called_once()
