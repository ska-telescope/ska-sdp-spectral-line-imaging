import warnings

import numpy as np
from matplotlib import pyplot as plt

warnings.filterwarnings("ignore")


def create_plot(*data, title, xlabel, ylabel, label, path):
    plt.plot(*data, ".", label=label)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    plt.rcParams["figure.figsize"] = [7.00, 3.50]
    plt.rcParams["figure.autolayout"] = True
    plt.savefig(path, bbox_inches="tight")
    plt.close()


def amp_vs_channel_plot(visibilities, title, path, label=None):
    vis_avg = visibilities.mean(dim=["time", "baseline_id"])

    create_plot(
        np.abs(vis_avg),
        title=title,
        xlabel="channel",
        ylabel="amp",
        label=label,
        path=path,
    )
