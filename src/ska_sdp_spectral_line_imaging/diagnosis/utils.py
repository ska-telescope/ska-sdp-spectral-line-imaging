import dask
import matplotlib as mpl
import numpy as np
import pandas as pd

mpl.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402


@dask.delayed
def create_plot(*data, title, xlabel, ylabel, label, path):
    plt.rcParams["figure.figsize"] = [7.00, 3.50]
    plt.rcParams["figure.autolayout"] = True
    fig, ax = plt.subplots()
    ax.plot(*data, ".", label=label)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def amp_vs_channel_plot(visibilities, title, path, label=""):
    vis_avg = visibilities.mean(dim=["time", "baseline_id"])

    return create_plot(
        np.abs(vis_avg),
        title=title,
        xlabel="channel",
        ylabel="amp",
        label=label,
        path=path,
    )


@dask.delayed
def store_spectral_csv(frequency, vis, path):
    pd.DataFrame(
        {
            "channel": frequency,
            "visibility": vis,
            "absolute visibility": np.abs(vis),
        }
    ).to_csv(path)
