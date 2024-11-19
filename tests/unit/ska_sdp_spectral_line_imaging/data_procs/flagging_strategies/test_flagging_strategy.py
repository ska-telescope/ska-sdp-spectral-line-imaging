from mock import mock_open, patch

from ska_sdp_spectral_line_imaging.data_procs.flagging_strategies import (
    FlaggingStrategy,
)

from .template_expansions import (
    default_all_true,
    default_no_flag_low_outliers,
    default_no_low_pass,
    default_no_original_mask,
)


def remove_empty_lines(file_content):
    return "\n".join(
        content
        for content in file_content.split("\n")
        if len(content.strip()) > 0
    )


def test_should_build_default_strategy_with_all_flags_true():
    builder = FlaggingStrategy(
        base_threshold=2.0,
        iteration_count=3,
        threshold_factor_step=4.0,
        transient_threshold_factor=5.0,
        threshold_timestep_rms=3.0,
        threshold_channel_rms=3.0,
        keep_original_flags=True,
        keep_outliers=True,
        do_low_pass=True,
        window_size=[5, 3],
        time_sigma=6.0,
        freq_sigma=7.0,
    )

    mock_file = mock_open()
    with patch("builtins.open", mock_file, create=True):
        builder.write("default_strategy.lua")

    mock_file.assert_called_once_with("default_strategy.lua", "w")
    file_handle = mock_file()

    file_handle.write.assert_called_once_with(default_all_true)


def test_should_build_default_strategy_with_no_low_pass():
    builder = FlaggingStrategy(
        base_threshold=2.0,
        iteration_count=3,
        threshold_factor_step=4.0,
        transient_threshold_factor=5.0,
        threshold_timestep_rms=3.0,
        threshold_channel_rms=3.0,
        keep_outliers=True,
        do_low_pass=False,
        window_size=[5, 3],
        time_sigma=6.0,
        freq_sigma=7.0,
    )

    mock_file = mock_open()
    with patch("builtins.open", mock_file, create=True):
        builder.write("default_strategy.lua")

    mock_file.assert_called_once_with("default_strategy.lua", "w")
    file_handle = mock_file()
    file_handle.write.assert_called_once_with(default_no_low_pass)


def test_should_build_default_strategy_with_no_flag_low_outliers():
    builder = FlaggingStrategy(
        base_threshold=2.0,
        iteration_count=3,
        threshold_factor_step=4.0,
        transient_threshold_factor=5.0,
        threshold_timestep_rms=3.0,
        threshold_channel_rms=3.0,
        keep_outliers=False,
        do_low_pass=True,
        window_size=[5, 3],
        time_sigma=6.0,
        freq_sigma=7.0,
    )

    mock_file = mock_open()
    with patch("builtins.open", mock_file, create=True):
        builder.write("default_strategy.lua")

    mock_file.assert_called_once_with("default_strategy.lua", "w")
    file_handle = mock_file()
    file_handle.write.assert_called_once_with(default_no_flag_low_outliers)


def test_should_build_default_strategy_with_no_original_flags():
    builder = FlaggingStrategy(
        base_threshold=2.0,
        iteration_count=3,
        threshold_factor_step=4.0,
        transient_threshold_factor=5.0,
        threshold_timestep_rms=3.0,
        threshold_channel_rms=3.0,
        keep_outliers=True,
        do_low_pass=True,
        window_size=[5, 3],
        time_sigma=6.0,
        freq_sigma=7.0,
    )

    mock_file = mock_open()
    with patch("builtins.open", mock_file, create=True):
        builder.write("default_strategy.lua")

    mock_file.assert_called_once_with("default_strategy.lua", "w")
    file_handle = mock_file()
    file_handle.write.assert_called_once_with(default_no_original_mask)
