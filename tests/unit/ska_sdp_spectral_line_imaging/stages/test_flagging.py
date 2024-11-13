# pylint: disable=import-error,import-outside-toplevel,no-member
import pytest
from mock import Mock, mock

import ska_sdp_spectral_line_imaging.stages.flagging as flag_dec
from ska_sdp_spectral_line_imaging.stages.flagging import (
    AOFLAGGER_AVAILABLE,
    flagging_stage,
)
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


def test_should_raise_exception_if_aoflagger_is_not_installed():
    actual_value = AOFLAGGER_AVAILABLE
    flag_dec.AOFLAGGER_AVAILABLE = False

    with pytest.raises(ImportError):
        flagging_stage.stage_definition("upstream_output", "strategy.lua")

    flag_dec.AOFLAGGER_AVAILABLE = actual_value


@pytest.mark.skipif(
    not AOFLAGGER_AVAILABLE, reason="AOFlagger is required for this test"
)
class TestFlagging:
    @mock.patch(
        "ska_sdp_spectral_line_imaging.stages.flagging.chunked_flagging"
    )
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.os")
    def test_should_raise_file_not_found_error_when_file_is_missing(
        self, os_mock, flagging_mock
    ):
        flagging_mock.return_value = "flagged values"
        upstream_output = UpstreamOutput()
        upstream_output["ps"] = Mock(name="ps")

        strategy_file = "strategy.lua"

        os_mock.path.exists.return_value = False

        with pytest.raises(FileNotFoundError) as err:
            flagging_stage.stage_definition(upstream_output, strategy_file)

        os_mock.path.exists.assert_called_once_with(strategy_file)

        assert (
            "The provided strategy file path strategy.lua does not not exists"
            in str(err.value)
        )

    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.os")
    @mock.patch(
        "ska_sdp_spectral_line_imaging.stages.flagging.chunked_flagging"
    )
    def test_should_flag_the_visibilities(self, flagging_mock, os_mock):

        ps = Mock(name="ps")
        strategy_file = "strategy_file.lua"
        upstream_output = UpstreamOutput()
        upstream_output["ps"] = ps
        ps.VISIBILITY = Mock(name="VISIBILITY")
        ps.FLAG = Mock(name="FLAG")
        ps.FLAG.dims = ["baseline", "time"]
        ps.FLAG.chunksizes = 2
        flagged_cube_mock = Mock(name="flagged cube")
        flagging_mock.return_value = flagged_cube_mock
        expand_dims_mock = Mock(return_value=flagged_cube_mock)
        flagged_cube_mock.expand_dims = expand_dims_mock
        transpose_mock = Mock(return_value=flagged_cube_mock)
        flagged_cube_mock.transpose = transpose_mock
        chunk_mock = Mock(name="chunk mock")
        flagged_cube_mock.chunk = chunk_mock

        ps.VISIBILITY.time.size = 10
        ps.VISIBILITY.frequency.size = 10
        ps.VISIBILITY.polarization.size = 2
        polarization_mock = Mock(name="polarization")
        ps.FLAG.polarization = polarization_mock
        vis_rechunked = Mock(name="vis rechunked")
        flag_rechunked = Mock(name="flag rechunked")

        ps.VISIBILITY.chunk.return_value = vis_rechunked
        ps.FLAG.chunk.return_value = flag_rechunked
        flagging_stage.stage_definition(upstream_output, strategy_file)

        flagging_mock.assert_called_once_with(
            vis_rechunked, flag_rechunked, 10, 10, 2, strategy_file
        )
        expand_dims_mock.assert_called_once_with(
            dim={"polarization": polarization_mock}
        )
        chunk_mock.assert_called_once_with(2)

        transpose_mock.assert_called_once_with(*ps.FLAG.dims)

    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.os")
    @mock.patch(
        "ska_sdp_spectral_line_imaging.stages.flagging.flagging_strategies."
        "__file__"
    )
    @mock.patch(
        "ska_sdp_spectral_line_imaging.stages.flagging.chunked_flagging"
    )
    def test_should_take_default_strategy_file_when_not_provided(
        self, flagging_mock, flagging_strategies_mock, os_mock
    ):

        ps = Mock(name="ps")
        os_mock.path.dirname.return_value = "path/to/dir"
        os_mock.path.abspath.return_value = "path"
        upstream_output = UpstreamOutput()
        upstream_output["ps"] = ps
        ps.VISIBILITY = Mock(name="VISIBILITY")
        ps.FLAG = Mock(name="FLAG")
        ps.FLAG.dims = ["baseline", "time"]
        ps.VISIBILITY.time.size = 10
        ps.VISIBILITY.frequency.size = 10
        ps.VISIBILITY.polarization.size = 2
        ps.VISIBILITY.chunk.return_value = "vis_rechunked"
        ps.FLAG.chunk.return_value = "flag_rechunked"
        flagging_stage.stage_definition(upstream_output, None)

        flagging_mock.assert_called_once_with(
            "vis_rechunked",
            "flag_rechunked",
            10,
            10,
            2,
            "path/to/dir/generic-default.lua",
        )

        os_mock.path.abspath.assert_called_once_with(flagging_strategies_mock)
        os_mock.path.dirname.assert_called_once_with("path")
