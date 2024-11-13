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
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.flag_cube")
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
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.rechunk")
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.flag_cube")
    def test_should_flag_the_visibilities(
        self, flagging_mock, rechunk_mock, os_mock
    ):
        ps = Mock(name="ps")
        strategy_file = "strategy_file.lua"
        upstream_output = UpstreamOutput()
        upstream_output["ps"] = ps
        polarization_mock = Mock(name="polarization")
        ps.FLAG.polarization = polarization_mock
        ps.assign = Mock(name="assign", return_value="NEW_PS")
        flagging_mock.return_value = "FLAGGED_CUBE"
        rechunk_mock.return_value = "RECHUNKED_FLAGS"

        output = flagging_stage.stage_definition(
            upstream_output, strategy_file
        )

        assert output["ps"] == "NEW_PS"

        flagging_mock.assert_called_once_with(ps, strategy_file)

        rechunk_mock.assert_called_once_with(
            "FLAGGED_CUBE", ps.FLAG, dim={"polarization": ps.FLAG.polarization}
        )

        ps.assign.assert_called_once_with({"FLAG": "RECHUNKED_FLAGS"})

    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.os")
    @mock.patch(
        "ska_sdp_spectral_line_imaging.stages.flagging.flagging_strategies."
        "__file__"
    )
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.rechunk")
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.flag_cube")
    def test_should_take_default_strategy_file_when_not_provided(
        self, flagging_mock, rechunk_mock, flagging_strategies_mock, os_mock
    ):

        ps = Mock(name="ps")
        os_mock.path.dirname.return_value = "path/to/dir"
        os_mock.path.abspath.return_value = "path"
        upstream_output = UpstreamOutput()
        upstream_output["ps"] = ps
        ps.VISIBILITY = Mock(name="VISIBILITY")
        ps.FLAG = Mock(name="FLAG")
        flagging_stage.stage_definition(upstream_output, None)

        flagging_mock.assert_called_once_with(
            ps,
            "path/to/dir/generic-default.lua",
        )

        os_mock.path.abspath.assert_called_once_with(flagging_strategies_mock)
        os_mock.path.dirname.assert_called_once_with("path")
