import os
import shutil
import tarfile

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
    for stoke in "IQUV":
        shutil.copy(f"{RESOURCE_DIR}/tMS-{stoke}-image.fits", tmp_path)
    shutil.copy(f"{RESOURCE_DIR}/test.config.yml", tmp_path)
    os.chdir(tmp_path)

    yield tmp_path


def test_pipeline(prepare_test_data):
    """
    Given a MSv4 and a model image the pipepile should output a stokes cube
    """

    output_dir = "./pipeline_output"
    os.makedirs(output_dir)

    spectral_line_imaging_pipeline.run(
        MSIN, output_dir, config_path="./test.config.yml"
    )

    assert len(output_dir) > 0
    assert os.path.isdir(f"./{output_dir}/test_cube.zarr")
    assert os.path.isdir(f"./{output_dir}/residual.zarr")
    assert os.path.isdir(f"./{output_dir}/model.zarr")
