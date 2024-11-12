import os
import shutil
import sys
import tarfile

import mock
import pytest

from ska_sdp_piper.piper.utils.io_utils import timestamp
from ska_sdp_spectral_line_imaging.pipeline import (
    spectral_line_imaging_pipeline,
)

MSIN = "./gmrt.ps"
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

    untar(f"{RESOURCE_DIR}/gmrt.ps.tgz", tmp_path)
    for pol in OUTPUT_POLS:
        shutil.copy(f"{RESOURCE_DIR}/gmrt-{pol}-image.fits", tmp_path)
    shutil.copy(f"{RESOURCE_DIR}/test.config.yml", tmp_path)
    os.chdir(tmp_path)

    yield tmp_path


def test_should_run_pipeline(prepare_test_data):
    """
    Given a processing set, a config file, and model FITS images
    in desired output polarization, when the pipeline is run by
    providing valid cli arguments,
    then it should generate desired output products.
    """
    time_stamp = timestamp()
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

    pipeline_output_path = (
        f"{output_dir}/spectral_line_imaging_pipeline_{time_stamp}"
    )
    assert os.path.exists(pipeline_output_path)

    expected_products = [
        "test_cube.model.fits",
        "test_cube.psf.fits",
        "test_cube.residual.fits",
        "test_cube.restored.fits",
        "residual.zarr",
        "model.zarr",
        f"spectral_line_imaging_pipeline_{timestamp()}.log",
        f"spectral_line_imaging_pipeline_{timestamp()}.config.yml",
        f"spectral_line_imaging_pipeline_{timestamp()}.cli.yml",
    ]
    actual_products = os.listdir(pipeline_output_path)
    expected_products.sort()
    actual_products.sort()
    assert expected_products == actual_products


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
    time_stamp = timestamp()
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
        "--set",
        "parameters.imaging.n_iter_major",
        "0",
        "--set",
        "parameters.continuum_subtraction.export_residual",
        "false",
        "--set",
        "parameters.predict_stage.export_model",
        "false",
    ]
    with mock.patch.object(sys, "argv", testargs):
        spectral_line_imaging_pipeline()

    pipeline_output_path = (
        f"{output_dir}/spectral_line_imaging_pipeline_{time_stamp}"
    )
    assert os.path.exists(pipeline_output_path)

    expected_products = [
        # TODO: model and residual exports can be turned off
        # for other tests as it is already convered once
        "test_cube.dirty.fits",
        f"spectral_line_imaging_pipeline_{timestamp()}.log",
        f"spectral_line_imaging_pipeline_{timestamp()}.config.yml",
        f"spectral_line_imaging_pipeline_{timestamp()}.cli.yml",
    ]
    actual_products = os.listdir(pipeline_output_path)
    expected_products.sort()
    actual_products.sort()
    assert expected_products == actual_products
