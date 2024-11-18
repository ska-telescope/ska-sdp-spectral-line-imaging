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
        flagging_stage.stage_definition(
            "upstream_output", "strategy.lua", {}, False, "", ""
        )

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
            flagging_stage.stage_definition(
                upstream_output, strategy_file, {}, False, "", ""
            )

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
            upstream_output, strategy_file, {}, False, "", ""
        )

        assert output["ps"] == "NEW_PS"

        flagging_mock.assert_called_once_with(ps, strategy_file)

        rechunk_mock.assert_called_once_with(
            "FLAGGED_CUBE", ps.FLAG, dim={"polarization": ps.FLAG.polarization}
        )

        ps.assign.assert_called_once_with({"FLAG": "RECHUNKED_FLAGS"})

    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.os")
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.rechunk")
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.flag_cube")
    @mock.patch(
        "ska_sdp_spectral_line_imaging.stages.flagging.FlaggingStrategy"
    )
    def test_should_take_get_default_strategy_when_strategy_file_not_provided(
        self,
        flagging_strategy_mock,
        flagging_mock,
        rechunk_mock,
        os_mock,
    ):

        ps = Mock(name="ps")
        upstream_output = UpstreamOutput()
        upstream_output["ps"] = ps
        ps.VISIBILITY = Mock(name="VISIBILITY")
        ps.FLAG = Mock(name="FLAG")

        builder_mock = Mock(name="builder")
        flagging_strategy_mock.return_value = builder_mock

        flagging_stage.stage_definition(
            upstream_output,
            None,
            {"config1": "values"},
            False,
            "",
            "output_path",
        )

        flagging_strategy_mock.assert_called_once_with(**{"config1": "values"})
        builder_mock.write.assert_called_once_with(
            "output_path/default_strategy.lua"
        )

        flagging_mock.assert_called_once_with(
            ps,
            "output_path/default_strategy.lua",
        )

    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.os")
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.rechunk")
    @mock.patch("ska_sdp_spectral_line_imaging.stages.flagging.flag_cube")
    @mock.patch(
        "ska_sdp_spectral_line_imaging.stages.flagging.export_to_zarr",
        return_value="DELAYED_EXPORT",
    )
    def test_should_export_flags(
        self, export_to_zarr_mock, flagging_mock, rechunk_mock, os_mock
    ):
        ps = Mock(name="ps")
        strategy_file = "strategy_file.lua"
        upstream_output = UpstreamOutput()
        upstream_output["ps"] = ps
        polarization_mock = Mock(name="polarization")
        ps.FLAG.polarization = polarization_mock

        ps.assign = Mock(name="assign", return_value=ps)
        flagging_mock.return_value = "FLAGGED_CUBE"
        rechunk_mock.return_value = "RECHUNKED_FLAGS"

        os_mock.path.join.return_value = "output/export/path"

        output = flagging_stage.stage_definition(
            upstream_output, strategy_file, {}, True, "export/path", "output"
        )

        export_to_zarr_mock.assert_called_once_with(
            ps.FLAG, "output/export/path", clear_attrs=True
        )
        assert "DELAYED_EXPORT" in output.compute_tasks
