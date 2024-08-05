import numpy as np
from matplotlib import pyplot as plt


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


def amp_vs_channel_plot(visibilities, title, path, all_stokes=False):
    vis_avg = visibilities.mean(dim=["time", "baseline_id"])
    label = ["I", "Q", "U", "V"]
    if not all_stokes:
        vis_avg = vis_avg.T[0]
        label = ["I"]

    create_plot(
        np.abs(vis_avg),
        title=title,
        xlabel="channel",
        ylabel="amp",
        label=label,
        path=path,
    )


def amp_vs_uv_distance_plot(uv_distance, visibilities, channel, title, path):
    vis = visibilities.mean(dim="time").T[0][channel]

    create_plot(
        np.abs(uv_distance),
        np.abs(vis),
        title=title,
        xlabel="channel",
        ylabel="amp",
        label="I",
        path=path,
    )
