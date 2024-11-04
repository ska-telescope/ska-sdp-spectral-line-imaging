import asyncio

import numpy as np
import pytest
from mock import MagicMock, Mock, patch

from ska_sdp_spectral_line_imaging.util import (
    estimate_cell_size,
    estimate_image_size,
    export_image_as,
    export_to_fits,
    export_to_zarr,
)


@patch("ska_sdp_spectral_line_imaging.util.export_to_zarr")
def test_should_export_image_as_zarr(export_to_zarr_mock):
    image = Mock(name="image")
    export_to_zarr_mock.return_value = "zarr_task"

    export_task = export_image_as(image, "output_path", export_format="zarr")

    export_to_zarr_mock.assert_called_once_with(image, "output_path")
    assert export_task == "zarr_task"


@patch("ska_sdp_spectral_line_imaging.util.export_to_fits")
def test_should_export_image_as_fits(export_to_fits_mock):
    image = Mock(name="image")
    export_to_fits_mock.return_value = "fits_task"

    loop = asyncio.get_event_loop()
    export_task = loop.run_until_complete(
        export_image_as(image, "output_path", export_format="fits")
    )

    export_to_fits_mock.assert_called_once_with(image, "output_path")
    assert export_task == "fits_task"


def test_should_throw_exception_for_unsupported_data_format():
    image = Mock(name="image")

    with pytest.raises(ValueError):
        export_image_as(image, "output_path", export_format="unsuported")


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


def test_should_export_image_to_fits_delayed():
    image = MagicMock(name="Image instance")

    export_to_fits(image, "output_dir/image_name").compute()

    image.image_acc.export_to_fits.assert_called_once_with(
        "output_dir/image_name.fits"
    )


def test_should_export_to_zarr_without_attrs():
    image = MagicMock(name="image")
    image_copy = MagicMock(name="image")
    image.copy.return_value = image_copy
    image_copy.to_zarr.return_value = "zarr_task"

    task = export_to_zarr(image, "output_path", clear_attrs=True)

    image.copy.assert_called_once_with(deep=False)
    image_copy.attrs.clear.assert_called_once()
    image_copy.to_zarr.assert_called_once_with(
        store="output_path.zarr", compute=False
    )
    assert task == "zarr_task"


def test_should_export_to_zarr_with_attrs():
    image = MagicMock(name="image")
    image_copy = MagicMock(name="image")
    image.copy.return_value = image_copy
    image_copy.to_zarr.return_value = "zarr_task"

    task = export_to_zarr(image, "output_path")

    image.copy.assert_called_once_with(deep=False)
    image_copy.attrs.clear.assert_not_called()
    image_copy.to_zarr.assert_called_once_with(
        store="output_path.zarr", compute=False
    )
    assert task == "zarr_task"


# TODO
# def test_get_wcs():
#

# TODO
# def test_pol_frame():
#     mock_pol_frame.assert_called_once_with("linearnp")
