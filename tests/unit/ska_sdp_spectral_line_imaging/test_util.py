import numpy as np
import xarray as xr

from ska_sdp_spectral_line_imaging.util import estimate_cell_size


def test_should_estimate_cell_size_in_arcsec():
    # inputs
    uvw = xr.DataArray(
        np.array(
            [
                [
                    [15031.69261419, 24845.31151338, 18263.42172039],
                    [193, 212, 220],
                ]
            ],
            dtype=np.float64,
        ),
        dims=("time", "baseline_id", "uvw_label"),
    )
    frequency = 6.34074219e08  # Hz
    factor = 3.0
    expected_cell_size = np.array([1.08204957, 0.65465215])

    # action
    actual_cell_size = estimate_cell_size(uvw, frequency, factor)

    # verify
    np.testing.assert_allclose(actual_cell_size, expected_cell_size)
