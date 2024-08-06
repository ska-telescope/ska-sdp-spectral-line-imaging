import os
import shutil
from pathlib import Path

import pytest

from ska_sdp_spectral_line_imaging.diagnosis import SpectralLineDiagnoser
from tests.integration.ska_sdp_spectral_line_imaging.test_pipeline import (
    RESOURCE_DIR,
    untar,
)


@pytest.fixture
def prepare_test_sandbox(tmp_path):
    untar(f"{RESOURCE_DIR}/diagnosis-input.tgz", tmp_path)
    untar(f"{RESOURCE_DIR}/tMS.ps.tgz", tmp_path)
    shutil.copy(f"{RESOURCE_DIR}/test.config.yml", tmp_path)
    os.chdir(tmp_path)
    yield tmp_path


def test_should_create_plots(prepare_test_sandbox):
    temp_path = prepare_test_sandbox

    timestamped_output_dir = Path("diagnosis-out/timestamped/")
    os.makedirs(timestamped_output_dir)

    diagnoser = SpectralLineDiagnoser(Path(temp_path), timestamped_output_dir)
    diagnoser.diagnose()

    expected_plots = [
        "all-stokes-amp-vs-channel-input-vis.png",
        "all-stokes-amp-vs-channel-model-vis.png",
        "all-stokes-amp-vs-channel-residual-vis.png",
        "amp-vs-uv-distance-after-cont-sub.png",
        "amp-vs-uv-distance-before-cont-sub.png",
        "single-stoke-i-amp-vs-channel-input-vis.png",
        "single-stoke-i-amp-vs-channel-model-vis.png",
        "single-stoke-i-amp-vs-channel-residual-vis.png",
    ]
    created_plots = os.listdir(timestamped_output_dir)
    created_plots.sort()
    expected_plots.sort()
    assert expected_plots == created_plots
