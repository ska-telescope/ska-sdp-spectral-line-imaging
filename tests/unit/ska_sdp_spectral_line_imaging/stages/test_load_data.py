from mock import mock
from mock.mock import Mock

from ska_sdp_spectral_line_imaging.stages.load_data import load_data
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.load_data.read_processing_set"
)
def test_should_load_data(read_processing_set_mock):
    ps = Mock(name="ps")
    ps.summary = Mock(name="ps-summar")
    ps.summary.return_value = ps.summary
    ps.summary.name = ["observation"]

    selected_ps = Mock(name="selected_ps")
    selected_ps.unify_chunks = Mock(
        name="unify-chunk", return_value="selected_ps"
    )
    ps.__getitem__ = Mock(name="ps-getitem", return_value=selected_ps)

    read_processing_set_mock.return_value = ps

    upstream_output = UpstreamOutput()

    actual = load_data.stage_definition(upstream_output, 0, {"input": "path"})

    assert actual.ps == "selected_ps"
    assert actual.input_data == ps

    ps.__getitem__.assert_called_once_with("observation")
    read_processing_set_mock.assert_called_once_with(ps_store="path")
