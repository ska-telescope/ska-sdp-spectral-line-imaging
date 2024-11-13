from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


def test_should_add_keys_to_upstream_output():
    upstream_output = UpstreamOutput()

    upstream_output["key"] = "VALUE"

    assert upstream_output["key"] == "VALUE"
    assert upstream_output.key == "VALUE"


def test_should_add_compute_tasks_to_upstream_output():
    upstream_output = UpstreamOutput()

    upstream_output.add_compute_tasks("TASK1", "TASK2")

    assert upstream_output.compute_tasks == ["TASK1", "TASK2"]
