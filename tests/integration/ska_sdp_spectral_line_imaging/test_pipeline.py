import os
import sys
from importlib.resources import files
from subprocess import check_call

import pytest

import tests.integration.ska_sdp_spectral_line_imaging.resources as resources
from ska_sdp_spectral_line_imaging.pipeline import (
    spectral_line_imaging_pipeline,
)

sys.path.append(".")

MSIN = "tMS.ps"
RESOURCEDIR = files(resources)


def untar(source):
    if not os.path.isfile(source):
        raise IOError(
            f"Not able to find {source} containing test input files."
        )
    check_call(["tar", "xf", source])


@pytest.fixture()
def run_in_tmp_path(tmp_path):
    """
    Creates a temporary directory, runs the test in it, and removes the
    directory.
    """
    # 'tmp_path' is a base fixture from Pytest that already
    # does everything else, including cleaning up.
    os.chdir(tmp_path)


@pytest.fixture(autouse=True)
def source_env(run_in_tmp_path):
    untar(f"{RESOURCEDIR}/{MSIN}.tgz")


def test_pipeline():
    """
    Given a MSv4 and a model image the pipepile should output a stokes cube
    """
    os.system(f"cp {RESOURCEDIR}/tMS-I-image.fits .")
    os.system(f"cp {RESOURCEDIR}/tMS-Q-image.fits .")
    os.system(f"cp {RESOURCEDIR}/tMS-U-image.fits .")
    os.system(f"cp {RESOURCEDIR}/tMS-V-image.fits .")

    spectral_line_imaging_pipeline(
        f"{RESOURCEDIR}/{MSIN}",
        config_path=f"{RESOURCEDIR}/config.yaml",
    )

    output_path = os.listdir("./output")

    assert len(output_path) > 0
    assert os.path.isdir(f"./output/{output_path[0]}/test_cube.zarr")
    assert os.path.isdir(f"./output/{output_path[0]}/residual.zarr")
