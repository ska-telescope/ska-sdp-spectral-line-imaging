import pytest
from mock import Mock, mock

import ska_sdp_spectral_line_imaging.data_procs.flagging as flag_dec
from ska_sdp_spectral_line_imaging.data_procs.flagging import (
    AOFLAGGER_AVAILABLE,
    chunked_flagging,
    flag_baseline,
    flag_cube,
)

# pylint: disable=import-error,import-outside-toplevel


def test_should_raise_exception_if_aoflagger_is_not_installed():
    actual_value = AOFLAGGER_AVAILABLE
    flag_dec.AOFLAGGER_AVAILABLE = False

    with pytest.raises(ImportError):
        flag_baseline("visibility", "flags", 5, 5, 2, "strategy.lua")

    flag_dec.AOFLAGGER_AVAILABLE = actual_value


@pytest.mark.skipif(
    not AOFLAGGER_AVAILABLE, reason="AOFlagger is required for this test"
)
class TestAOFlagger:
    @mock.patch(
        "ska_sdp_spectral_line_imaging.data_procs.flagging.chunked_flagging",
        return_value="VISIBILITY_FLAGS",
    )
    def test_should_flag_the_visibilities(self, flagging_mock):

        ps = Mock(name="ps")
        strategy_file = "strategy_file.lua"

        ps.FLAG = Mock(name="FLAG")
        ps.FLAG.dims = ["baseline", "time"]
        ps.FLAG.chunksizes = 2
        ps.FLAG.chunk = Mock(name="chunk_flag", return_value="CHUNKED_FLAG")

        ps.VISIBILITY = Mock(name="VISIBILITY")
        ps.VISIBILITY.chunk = Mock(
            name="chunk_vis", return_value="CHUNKED_VIS"
        )
        ps.VISIBILITY.time = Mock(name="time")
        ps.VISIBILITY.frequency = Mock(name="frequency")
        ps.VISIBILITY.polarization = Mock(name="polarization")

        ps.VISIBILITY.time.size = 10
        ps.VISIBILITY.frequency.size = 10
        ps.VISIBILITY.polarization.size = 2

        output = flag_cube(ps, strategy_file)

        flagging_mock.assert_called_once_with(
            "CHUNKED_VIS", "CHUNKED_FLAG", 10, 10, 2, strategy_file
        )
        ps.VISIBILITY.chunk.assert_called_once_with(
            {"baseline_id": 1, "frequency": 10}
        )
        ps.FLAG.chunk.assert_called_once_with(
            {"baseline_id": 1, "frequency": 10}
        )

        assert output == "VISIBILITY_FLAGS"

    @mock.patch(
        "ska_sdp_spectral_line_imaging.data_procs.flagging.xr.apply_ufunc"
    )
    def test_should_apply_flagging(self, apply_ufunc_mock):

        visibility_mock = "visibility"
        flags_mock = "flags"

        chunked_flagging(visibility_mock, flags_mock, 5, 5, 2, "strategy.lua")

        apply_ufunc_mock.assert_called_once_with(
            flag_baseline,
            visibility_mock,
            flags_mock,
            input_core_dims=[
                ["polarization", "frequency", "time"],
                ["polarization", "frequency", "time"],
            ],
            output_core_dims=[["frequency", "time"]],
            vectorize=True,
            output_dtypes=[bool],
            dask="parallelized",
            keep_attrs=True,
            dask_gufunc_kwargs={
                "output_sizes": {"time": 5, "frequency": 5},
            },
            kwargs=dict(
                ntime=5, nchan=5, npol=2, strategy_file="strategy.lua"
            ),
        )

    @mock.patch(
        "ska_sdp_spectral_line_imaging.data_procs.flagging.aoflagger.AOFlagger"
    )
    def test_should_do_flagging_on_chunked_visibilities(self, aoflagger_mock):
        flagger_mock = Mock(name="flagger")
        mask_mock = Mock(name="mask")
        data_mock = Mock(name="data")
        strategy_mock = Mock(name="strategy")
        aoflagger_mock.return_value = flagger_mock
        load_strategy_file_mock = Mock(name="load strategy file")
        load_strategy_file_mock.return_value = strategy_mock
        make_image_set_mock = Mock(name="make image set")
        make_image_set_mock.return_value = data_mock
        make_flag_mask_mock = Mock(name="make flag mask")
        make_flag_mask_mock.return_value = mask_mock

        flagger_mock.load_strategy_file = load_strategy_file_mock
        flagger_mock.make_flag_mask = make_flag_mask_mock
        flagger_mock.make_image_set = make_image_set_mock
        visibility_mock = [1 + 2j, 2 + 3j]
        flags_mock = ["flag1", "flag2"]

        flag_baseline(visibility_mock, flags_mock, 5, 5, 2, "strategy.lua")

        load_strategy_file_mock.assert_called_once_with("strategy.lua")
        make_image_set_mock.assert_called_once_with(5, 5, 2 * 2)
        data_mock.set_image_buffer.assert_has_calls(
            [
                mock.call(0, 1),
                mock.call(1, 2),
                mock.call(2, 2),
                mock.call(3, 3),
            ],
        )
        make_flag_mask_mock.assert_called_once_with(5, 5, False)

        mask_mock.set_buffer.assert_called_once_with("flag1")

        strategy_mock.run.assert_called_once_with(data_mock, mask_mock)
