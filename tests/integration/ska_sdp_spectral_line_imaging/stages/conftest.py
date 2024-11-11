import numpy as np
import pytest
import xarray as xr


@pytest.fixture(name="result_msv4")
def fixture_msv4():
    """
    Generate a MSv4 processing set for unit tests
    """
    num_times = 8
    num_baselines = 21
    num_pols = 1

    frequency = np.array(
        [1.01e8, 1.02e8, 1.03e8, 1.04e8, 1.05e8, 1.06e8, 1.07e8, 1.08e8]
    )
    time = np.array([10.0, 20.0, 30.0, 40.0, 50.0, 60, 70, 80])
    vis = (1 + 2j) * np.ones(
        shape=(len(time), num_baselines, len(frequency), num_pols)
    ).astype(complex)
    weight = np.ones(
        shape=(len(time), num_baselines, len(frequency), num_pols)
    ).astype(np.float32)
    uvw = np.zeros((num_times, num_baselines, 3))
    flag = np.zeros(
        shape=(len(time), num_baselines, len(frequency), num_pols)
    ).astype(bool)
    flag[2:4, :, 4:7, :] = True
    for t in range(num_times):
        for b in range(num_baselines):
            uvw[t, b, 0] = 1.1 + t * 0.1 + b * 0.1
            uvw[t, b, 1] = 1.2 + t * 0.1 + b * 0.1
            uvw[t, b, 2] = 1.3 + t * 0.1 + b * 0.1

    baselines = np.arange(21, dtype=np.int16)

    ps = xr.Dataset(
        {
            "VISIBILITY": (
                ("time", "baseline_id", "frequency", "polarization"),
                vis,
            ),
            "WEIGHT": (
                ("time", "baseline_id", "frequency", "polarization"),
                weight,
            ),
            "UVW": (("time", "baseline_id", "uvw_label"), uvw),
            "FLAG": (
                ("time", "baseline_id", "frequency", "polarization"),
                flag,
            ),
        },
        {
            "time": time,
            "baseline_id": baselines,
            "frequency": frequency,
            "polarization": np.array(["I"]),
            "uvw_label": np.array(["u", "v", "w"]),
        },
    )

    return ps
