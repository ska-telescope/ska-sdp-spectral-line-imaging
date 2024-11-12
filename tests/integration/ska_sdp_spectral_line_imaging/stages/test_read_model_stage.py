import os

from ska_sdp_spectral_line_imaging.stages.model import read_model
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput

RESOURCE_DIR = f"{os.path.dirname(os.path.realpath(__file__))}/../resources"


def test_read_model_stage(result_msv4):
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = result_msv4.chunk(frequency=2)

    read_model.stage_definition(
        upstream_output,
        f"{RESOURCE_DIR}/tMS-%s-image.fits",
        do_power_law_scaling=True,
        spectral_index=True,
    )
