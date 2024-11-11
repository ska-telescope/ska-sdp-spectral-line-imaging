import os
import shutil
import sys
import tarfile

import mock
import pytest

from ska_sdp_spectral_line_imaging.pipeline import (
    spectral_line_imaging_pipeline,
)

MSIN = "./tMS.ps"
RESOURCE_DIR = f"{os.path.dirname(os.path.realpath(__file__))}/resources"


def untar(source, dest):
    tar = tarfile.open(source)
    tar.extractall(path=dest)
    tar.close()


@pytest.fixture()
def prepare_test_data(tmp_path):
    """
    Creates a temporary directory, runs the test in it, and removes the
    directory.
    """
    # 'tmp_path' is a base fixture from Pytest that already
    # does everything else, including cleaning up.

    untar(f"{RESOURCE_DIR}/tMS.ps.tgz", tmp_path)
    for stoke in ["I", "Q", "U", "V"]:
        shutil.copy(f"{RESOURCE_DIR}/tMS-{stoke}-image.fits", tmp_path)
    shutil.copy(f"{RESOURCE_DIR}/test.config.yml", tmp_path)
    os.chdir(tmp_path)

    yield tmp_path


def test_pipeline(prepare_test_data):
    """
    Given a MSv4 and a model image the pipepile should output a stokes cube
    """
    output_dir = f"{prepare_test_data}/pipeline_output"

    testargs = [
        "test",
        "run",
        "--input",
        MSIN,
        "--output",
        output_dir,
        "--config",
        "./test.config.yml",
    ]
    with mock.patch.object(sys, "argv", testargs):
        spectral_line_imaging_pipeline()

    list_dir = os.listdir(output_dir)

    assert len(os.listdir(output_dir)) == 1

    pipeline_output = f"{output_dir}/{list_dir[0]}"

    assert os.path.exists(f"{pipeline_output}/test_cube.dirty.fits")
    assert os.path.isdir(f"{pipeline_output}/residual.zarr")
    assert os.path.isdir(f"{pipeline_output}/model.zarr")
