from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stages.model import cont_sub
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
def test_should_not_report_peak_channel_value(
    numpy_mock, subtract_visibility_mock
):

    observation = Mock(name="observation")
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation

    cont_sub.stage_definition(upstream_output, False)

    numpy_mock.abs.assert_not_called()
