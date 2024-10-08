import numpy as np

from ska_sdp_spectral_line_imaging.util import (
    estimate_cell_size,
    estimate_image_size,
)


def test_should_estimate_cell_size_in_arcsec():
    # inputs
    baseline = 15031.69261419  # meters
    wavelength = 0.47313073298  # meters
    factor = 3.0
    expected_cell_size = 1.08  # rounded to 2 decimals

    # action
    actual_cell_size = estimate_cell_size(baseline, wavelength, factor)

    # verify
    np.testing.assert_array_equal(actual_cell_size, expected_cell_size)


def test_should_estimate_image_size():
    # inputs
    maximum_wavelength = 0.48811594
    antenna_diameter = 45.0
    cell_size = 0.65465215
    expected_image_size = 5200  # rounded to greater multiple of 100

    # action
    actual_image_size = estimate_image_size(
        maximum_wavelength, antenna_diameter, cell_size
    )

    # verify
    np.testing.assert_array_equal(actual_image_size, expected_image_size)
