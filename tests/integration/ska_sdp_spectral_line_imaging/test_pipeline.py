import os
import shutil
import tarfile

import pytest

from ska_sdp_piper.piper.utils.io_utils import timestamp
from ska_sdp_spectral_line_imaging.pipeline import (
    spectral_line_imaging_pipeline,
)

MSIN = "./gmrt.small.ps"
OUTPUT_POLS = ["I", "V"]
RESOURCE_DIR = f"{os.path.dirname(os.path.realpath(__file__))}/resources"


def untar(source, dest):
    tar = tarfile.open(source)
    tar.extractall(path=dest)
    tar.close()


@pytest.fixture(scope="function")
def prepare_test_data(tmp_path):
    """
    Creates a temporary directory, runs the test in it, and removes the
    directory.
    """
    # 'tmp_path' is a base fixture from Pytest that already
    # does everything else, including cleaning up.

    untar(f"{RESOURCE_DIR}/gmrt.small.ps.tgz", tmp_path)
    for pol in OUTPUT_POLS:
        shutil.copy(f"{RESOURCE_DIR}/gmrt-{pol}-image.fits", tmp_path)
    shutil.copy(f"{RESOURCE_DIR}/test.config.yml", tmp_path)
    shutil.copy(f"{RESOURCE_DIR}/test.dirty.config.yml", tmp_path)
    os.chdir(tmp_path)

    yield tmp_path


def test_should_run_pipeline(prepare_test_data):
    """
    Given a processing set, a config file, and model FITS images
    in desired output polarization, when we run the pipeline by
    providing valid inputs,
    then it should generate output products depending on the config.
    """
    output_dir = os.path.join(
        "./pipeline_output",
        f"{spectral_line_imaging_pipeline.name}_{timestamp()}",
    )
    os.makedirs(output_dir)

    spectral_line_imaging_pipeline.run(
        output_dir,
        config_path="./test.config.yml",
        cli_args={"input": MSIN},
    )

    expected_artifacts = [
        "test_cube.model.fits",
        "test_cube.psf.fits",
        "test_cube.residual.fits",
        "test_cube.restored.fits",
        "residual.zarr",
        "model.zarr",
        f"spectral_line_imaging_pipeline_{timestamp()}.log",
        f"spectral_line_imaging_pipeline_{timestamp()}.config.yml",
    ]
    expected_artifacts.sort()
    created_artifacts = os.listdir(output_dir)
    created_artifacts.sort()
    assert expected_artifacts == created_artifacts


# TODO: Once piper allows modifying configs are runtime
# Delete `dirty.config.yml``, use default `test.config.yml`, and
# change required params directly with equivalent of `--set`
def test_should_generate_dirty_image_when_niter_major_is_zero(
    prepare_test_data,
):
    """
    Given a processing set, a config, and model FITS images
    in desired output polarization are provided to the pipeline,
    when number of major iterations is set to 0,
    then the pipeline should generate only dirty image as the output of
    imaging stage.
    """
    output_dir = os.path.join(
        "./pipeline_output",
        f"{spectral_line_imaging_pipeline.name}_{timestamp()}",
    )
    os.makedirs(output_dir)

    spectral_line_imaging_pipeline.run(
        output_dir,
        config_path="./test.dirty.config.yml",
        cli_args={"input": MSIN},
    )

    assert os.path.exists(
        f"./pipeline_output/spectral_line_imaging_pipeline_{timestamp()}/"
        "test_cube.dirty.fits"
    )


# TODO : Add a test for radler deconvolver if required
# from ska_sdp_spectral_line_imaging.stubs.deconvolution.radler import (
#     RADLER_AVAILABLE,
# )
# @pytest.mark.skipif(
#     not RADLER_AVAILABLE, reason="Radler is required for this test"
# )
# def test_should_run_with_radler(prepare_test_data):
#     pass
