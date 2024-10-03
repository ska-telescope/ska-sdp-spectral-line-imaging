import numpy as np

from ska_sdp_spectral_line_imaging.util import estimate_cell_size


def test_should_estimate_cell_size_in_arcsec():
    # inputs
    uvw = 15031.69261419
    frequency = 6.34074219e08  # Hz
    factor = 3.0
    expected_cell_size = 1.08204957

    # action
    actual_cell_size = estimate_cell_size(uvw, frequency, factor)

    # verify
    np.testing.assert_allclose(actual_cell_size, expected_cell_size)
