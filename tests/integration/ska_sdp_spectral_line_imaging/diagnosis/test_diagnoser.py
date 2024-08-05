import os

import mock
import pytest
from mock.mock import Mock

from ska_sdp_spectral_line_imaging.diagnosis import SpectralLineDiagnoser
from tests.integration.ska_sdp_spectral_line_imaging.test_pipeline import (
    RESOURCE_DIR,
    untar,
)


@pytest.fixture
def prepare_test_sandbox(tmp_path):
    untar(f"{RESOURCE_DIR}/diagnosis-input.tgz", tmp_path)
    untar(f"{RESOURCE_DIR}/tMS.ps.tgz", tmp_path)
    os.chdir(tmp_path)
    yield tmp_path


@mock.patch(
    "ska_sdp_spectral_line_imaging.diagnosis"
    + ".spectral_line_diagnoser.create_output_dir",
    return_value="diagnosis-out/timestamped",
)
def test_should_create_plots(create_output_dir_mock, prepare_test_sandbox):
    cli_args = Mock("cli-args")
    temp_path = prepare_test_sandbox
    cli_args.input = temp_path
    cli_args.output = "./diagnosis-out"

    os.makedirs("diagnosis-out/timestamped/")

    diagnoser = SpectralLineDiagnoser()
    diagnoser.diagnose(cli_args)

    plots = [
        "amp-vs-channel-input-vis.png",
        "amp-vs-channel-model-vis.png",
        "amp-vs-channel-residual-vis.png",
        "amp-vs-uv-distance-after-cont-sub.png",
        "amp-vs-uv-distance-before-cont-sub.png",
        "single-stoke-i-amp-vs-channel-input-vis.png",
        "single-stoke-i-amp-vs-channel-model-vis.png",
        "single-stoke-i-amp-vs-channel-residual-vis.png",
    ]

    assert plots.sort() == os.listdir("diagnosis-out/timestamped/").sort()
