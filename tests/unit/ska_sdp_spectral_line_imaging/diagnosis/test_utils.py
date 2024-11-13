import mock
from mock.mock import MagicMock, Mock

from ska_sdp_spectral_line_imaging.diagnosis.utils import (
    amp_vs_channel_plot,
    create_plot,
    store_spectral_csv,
)


@mock.patch("ska_sdp_spectral_line_imaging.diagnosis.utils.plt")
def test_should_create_plot(plt_mock):
    fig = MagicMock(name="fig")
    ax = MagicMock(name="ax")
    plt_mock.subplots.return_value = [fig, ax]

    create_plot(
        "xdata",
        "ydata",
        xlabel="x axis label",
        ylabel="y axis label",
        title="Title of the plot",
        path="output/image.png",
        label=None,
    ).compute()

    ax.plot.assert_called_once_with("xdata", "ydata", ".", label=None)
    ax.set_title.assert_called_once_with("Title of the plot")
    ax.set_xlabel.assert_called_once_with("x axis label")
    ax.set_ylabel.assert_called_once_with("y axis label")
    fig.savefig.assert_called_once_with(
        "output/image.png", bbox_inches="tight"
    )
    plt_mock.close.assert_called_once_with(fig)


@mock.patch("ska_sdp_spectral_line_imaging.diagnosis.utils.create_plot")
@mock.patch("ska_sdp_spectral_line_imaging.diagnosis.utils.np")
def test_should_plot_amp_vs_channel_plot(numpy_mock, create_plot_mock):
    visibilities = Mock(name="visibilities")
    visibilities.mean.return_value = "mean_visibilities"
    label = ["I", "Q"]
    numpy_mock.abs.return_value = "absolute_visibility"

    amp_vs_channel_plot(visibilities, "title", "path", label=label)

    visibilities.mean.assert_called_once_with(dim=["time", "baseline_id"])
    numpy_mock.abs.assert_called_once_with("mean_visibilities")
    create_plot_mock.assert_called_once_with(
        "absolute_visibility",
        title="title",
        xlabel="channel",
        ylabel="amp",
        label=["I", "Q"],
        path="path",
    )


@mock.patch("ska_sdp_spectral_line_imaging.diagnosis.utils.pd")
@mock.patch("ska_sdp_spectral_line_imaging.diagnosis.utils.np")
def test_should_store_spectral_csv(np_mock, pd_mock):
    pandas_df = MagicMock(name="to_csv")
    pd_mock.DataFrame.return_value = pandas_df
    np_mock.abs.return_value = "abs_vis"

    store_spectral_csv("frequency", "vis", "output/spectral.csv").compute()

    np_mock.abs.assert_called_once_with("vis")
    pd_mock.DataFrame.assert_called_once_with(
        {
            "channel": "frequency",
            "visibility": "vis",
            "absolute visibility": "abs_vis",
        }
    )
    pandas_df.to_csv.assert_called_once_with("output/spectral.csv")
