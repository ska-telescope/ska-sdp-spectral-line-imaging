import dask
import matplotlib as mpl
import numpy as np
import pandas as pd

mpl.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402


@dask.delayed
def create_plot(*data, title, xlabel, ylabel, label, path):
    fig = plt.figure()
    plt.plot(*data, ".", label=label)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    plt.rcParams["figure.figsize"] = [7.00, 3.50]
    plt.rcParams["figure.autolayout"] = True
    fig.savefig(path, bbox_inches="tight")


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
