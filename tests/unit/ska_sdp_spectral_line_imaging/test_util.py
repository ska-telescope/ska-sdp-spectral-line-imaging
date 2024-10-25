import asyncio

import numpy as np
import pytest
from mock import Mock, patch

from ska_sdp_spectral_line_imaging.util import (
    estimate_cell_size,
    estimate_image_size,
    export_data_as,
    export_to_fits,
)


def test_should_export_data_as_zarr():
    image = Mock(name="image")
    image.copy = Mock(name="copy", return_value=image)
    image.to_zarr = Mock(name="to_zarr", return_value="zarr_task")
    export_task = export_data_as(image, "output_path", export_format="zarr")

    image.copy.assert_called_once()
    image.attrs.clear.assert_called_once()
    image.to_zarr.assert_called_once_with(
        store="output_path.zarr", compute=False
    )

    assert export_task == "zarr_task"


@patch("ska_sdp_spectral_line_imaging.util.export_to_fits")
def test_should_export_data_as_fits(export_to_fits_mock):
    loop = asyncio.get_event_loop()

    image = Mock(name="image")
    export_to_fits_mock.return_value = "fits_task"

    export_task = loop.run_until_complete(
        export_data_as(image, "output_path", export_format="fits")
    )

    export_to_fits_mock.assert_called_once_with(image, "output_path")

    assert export_task == "fits_task"


def test_should_throw_exception_for_unsupported_data_format():

    image = Mock(name="image")

    with pytest.raises(ValueError):
        export_data_as(image, "output_path", export_format="unsuported")


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


@patch("ska_sdp_spectral_line_imaging.util.fits")
def test_should_export_to_fits(mock_fits):
    cube = Mock(name="cube_data")
    hdu_mock = Mock(name="hdu")
    mock_fits.PrimaryHDU.return_value = hdu_mock

    export_to_fits(cube, "output_dir/image_name").compute()

    mock_fits.PrimaryHDU.assert_called_once_with(
        data=cube.pixels, header=cube.image_acc.wcs.to_header()
    )

    hdu_mock.writeto.assert_called_once_with("output_dir/image_name.fits")


# TODO
# def test_get_wcs():
#

# TODO
# def test_pol_frame():
#     mock_pol_frame.assert_called_once_with("linearnp")
