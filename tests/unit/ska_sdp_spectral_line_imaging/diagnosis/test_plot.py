import mock
import numpy as np
from mock.mock import Mock

from ska_sdp_spectral_line_imaging.diagnosis.plot import amp_vs_channel_plot


@mock.patch("ska_sdp_spectral_line_imaging.diagnosis.plot.create_plot")
def test_should_plot_single_stock_i(create_plot_mock):
    visibilities = Mock(name="visibilities")
    visibilities.mean.return_value = np.array([[1, 2], [3, 4]])

    amp_vs_channel_plot(visibilities, "title", "path")

    actual_vis_arg = create_plot_mock.call_args_list[0].args[0]
    actual_label_arg = create_plot_mock.call_args_list[0].kwargs["label"]
    np.testing.assert_array_equal(actual_vis_arg, np.array([1, 3]))

    assert actual_label_arg == ["I"]


@mock.patch("ska_sdp_spectral_line_imaging.diagnosis.plot.create_plot")
def test_should_plot_all_stocks(create_plot_mock):
    visibilities = Mock(name="visibilities")
    visibilities.mean.return_value = np.array([[1, 2], [3, 4]])

    amp_vs_channel_plot(visibilities, "title", "path", True)

    actual_vis_arg = create_plot_mock.call_args_list[0].args[0]
    actual_label_arg = create_plot_mock.call_args_list[0].kwargs["label"]
    np.testing.assert_array_equal(actual_vis_arg, np.array([[1, 2], [3, 4]]))

    assert actual_label_arg == ["I", "Q", "U", "V"]
