# pylint: disable=no-member
import asyncio

import numpy as np
import pytest
import xarray as xr
from mock import MagicMock, Mock, call, patch
from ska_sdp_datamodels.science_data_model.polarisation_model import (
    PolarisationFrame,
)

from ska_sdp_spectral_line_imaging.util import (
    estimate_cell_size_in_arcsec,
    estimate_image_size,
    export_image_as,
    export_to_fits,
    export_to_zarr,
    get_dask_array_from_fits,
    get_dataarray_from_fits,
    get_polarization_frame_from_observation,
    get_wcs_from_observation,
    read_fits_memmapped_delayed,
    rechunk,
)


def test_should_rechunk_data_array():
    data_array = Mock(name="data_array", spec=xr.DataArray)
    data_array.expand_dims.return_value = data_array
    data_array.transpose.return_value = data_array
    data_array.chunk.return_value = "RECHUNKED_DATA_ARRAY"

    ref = Mock(name="reference_data_array", spec=xr.DataArray)
    ref.dims = ["coord_1", "coord_2"]
    ref.chunksizes = 2

    output = rechunk(data_array, ref, {"coord": "value_for_coord"})

    data_array.expand_dims.assert_called_once_with(
        dim={"coord": "value_for_coord"}
    )

    data_array.transpose.assert_called_once_with("coord_1", "coord_2")
    data_array.chunk.assert_called_once_with(2)

    assert output == "RECHUNKED_DATA_ARRAY"


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
    actual_cell_size = estimate_cell_size_in_arcsec(
        baseline, wavelength, factor
    )

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


def test_should_get_polarization_frame_from_observation():
    obs = MagicMock(name="observation")
    obs.polarization.data = ["I", "Q", "U", "V"]

    expected_pol_frame = PolarisationFrame("stokesIQUV")

    actual_pol_frame = get_polarization_frame_from_observation(obs)

    assert actual_pol_frame == expected_pol_frame


@patch("ska_sdp_spectral_line_imaging.util.SkyCoord")
@patch("ska_sdp_spectral_line_imaging.util.au")
@patch("ska_sdp_spectral_line_imaging.util.WCS")
def test_should_get_wcs_from_observation_with_pol_I(
    wcs_mock, astro_unit_mock, sky_coord_mock
):
    obs = MagicMock(name="observation")

    field_phase_center = obs.VISIBILITY.field_and_source_xds.FIELD_PHASE_CENTER
    field_phase_center.sky_dir_label = xr.DataArray(["ra", "dec"])
    field_phase_center.units = ["rad", "rad"]
    field_phase_center.frame = "fk5"
    field_phase_center.to_numpy.return_value = np.array([-5.0, 5.0])

    obs.frequency.channel_width = {"data": 65000}
    obs.frequency.reference_frequency = {"data": 10000}
    obs.frequency.units = ["Hz"]
    obs.frequency.frame = "TOPO"
    obs.polarization.size = 1
    obs.polarization.data = ["I"]

    sky_coord = MagicMock(name="sky coordinate")
    sky_coord.ra.deg = 60.0
    sky_coord.dec.deg = 70.0

    sky_coord_mock.return_value = sky_coord
    # setting "rad" unit to random value
    astro_unit_mock.rad = 2.0

    actual_wcs = get_wcs_from_observation(obs, 3600.0, 256, 256)

    sky_coord_mock.assert_called_once_with(ra=-10.0, dec=10.0, frame="fk5")
    wcs_mock.assert_called_once_with(naxis=4)
    assert actual_wcs.wcs.crpix == [128, 128, 1, 1]
    assert actual_wcs.wcs.cunit == ["deg", "deg", "", "Hz"]
    assert actual_wcs.wcs.cdelt == [-1.0, 1.0, 1, 65000]
    assert actual_wcs.wcs.crval == [60.0, 70.0, 1, 10000]
    assert actual_wcs.wcs.specsys == "TOPO"
    # constant asserts which may change in future
    assert actual_wcs.wcs.ctype == ["RA---SIN", "DEC--SIN", "STOKES", "FREQ"]
    assert actual_wcs.wcs.radesys == "ICRS"
    assert actual_wcs.wcs.equinox == 2000.0


def test_should_raise_value_error_if_coord_label_is_not_ra_dec():
    obs = MagicMock(name="observation")
    field_phase_center = obs.VISIBILITY.field_and_source_xds.FIELD_PHASE_CENTER
    field_phase_center.sky_dir_label = xr.DataArray(["ra"])

    with pytest.raises(ValueError) as err:
        get_wcs_from_observation(obs, 3600.0, 256, 256)

    assert (
        str(err.value)
        == "Phase field center coordinates labels are not equal to RA / DEC."
    )


def test_should_raise_value_error_if_phase_center_unit_is_not_rad():
    obs = MagicMock(name="observation")
    field_phase_center = obs.VISIBILITY.field_and_source_xds.FIELD_PHASE_CENTER
    field_phase_center.sky_dir_label = xr.DataArray(["ra", "dec"])
    field_phase_center.units = ["deg"]

    with pytest.raises(ValueError) as err:
        get_wcs_from_observation(obs, 3600.0, 256, 256)

    assert (
        str(err.value) == "Phase field center value is not defined in radian."
    )


@patch("ska_sdp_spectral_line_imaging.util.fits.open")
def test_should_read_data_from_fits_delayed(fits_open_mock):
    hud1 = MagicMock(name="hud1")
    hud1.data = "data"
    fits_open_mock.return_value = fits_open_mock
    fits_open_mock.__enter__.return_value = ["hud0", hud1]

    output = read_fits_memmapped_delayed("/path/image", 1).compute()

    fits_open_mock.assert_called_once_with(
        "/path/image", mode="denywrite", memmap=True, lazy_load_hdus=True
    )
    assert output == "data"


@patch("ska_sdp_spectral_line_imaging.util." "read_fits_memmapped_delayed")
@patch("ska_sdp_spectral_line_imaging.util.dask.array")
def test_should_get_dask_array_from_fits(dask_array_mock, read_fits_mem_mock):
    dask_array_mock.from_delayed.return_value = "dask_array"
    read_fits_mem_mock.return_value = "fits_data"

    loop = asyncio.get_event_loop()

    output = get_dask_array_from_fits(
        "/path/image",
        1,
        (16, 16),
        ">f8",
    )

    read_fits_mem_mock.assert_called_once_with("/path/image", 1)

    read_fits_delayed_task = dask_array_mock.from_delayed.call_args.args[0]
    task_output = loop.run_until_complete(read_fits_delayed_task)
    assert task_output == "fits_data"
    assert dask_array_mock.from_delayed.call_args.kwargs["shape"] == (16, 16)
    assert dask_array_mock.from_delayed.call_args.kwargs["dtype"] == ">f8"

    assert output == "dask_array"


@patch("ska_sdp_spectral_line_imaging.util.get_dask_array_from_fits")
@patch("ska_sdp_spectral_line_imaging.util.fits.open")
@patch("ska_sdp_spectral_line_imaging.util.WCS")
def test_should_read_fits_image_when_fits_is_4d(
    wcs_mock, fits_open_mock, get_dask_array_from_fits_mock
):
    hud0 = MagicMock(name="fits_hud_0")
    hud0.data.shape = (2, 3, 1, 1)  # pol, freq, y / dec, x / ra
    hud0.data.dtype = ">f4"

    wcs = MagicMock(name="wcs")
    wcs.axis_type_names = ["RA", "DEC", "FREQ", "STOKES"]
    spectral_wcs = MagicMock(name="spectral_wcs")
    spectral_wcs.pixel_shape = [3]
    spectral_wcs.wcs_pix2world.return_value = [
        [10.0, 20.0, 30.0]
    ]  # frequencies
    stokes_wcs = MagicMock(name="stokes_wcs")
    stokes_wcs.pixel_shape = [2]
    stokes_wcs.wcs_pix2world.return_value = [[-7, -8]]  # fits_codes
    wcs.sub.side_effect = [spectral_wcs, stokes_wcs]

    fits_open_mock.return_value = fits_open_mock
    fits_open_mock.__enter__.return_value = [hud0]
    wcs_mock.return_value = wcs
    get_dask_array_from_fits_mock.return_value = (
        np.arange(6).reshape(hud0.data.shape).astype(np.float32)
    )

    expected_xrda = xr.DataArray(
        np.array(
            [[[[0.0]], [[1.0]], [[2.0]]], [[[3.0]], [[4.0]], [[5.0]]]],
        ),
        dims=["polarization", "frequency", "y", "x"],
        coords={"polarization": ["XY", "YX"], "frequency": [10.0, 20.0, 30.0]},
    )

    output_xrda = get_dataarray_from_fits("image_path", 0)

    fits_open_mock.assert_called_once_with("image_path", memmap=True)
    wcs_mock.assert_called_once_with("image_path")
    wcs.sub.assert_has_calls([call(["spectral"]), call(["stokes"])])
    spectral_wcs.wcs_pix2world.assert_called_once_with(range(0, 3), 0)
    stokes_wcs.wcs_pix2world.assert_called_once_with(range(0, 2), 0)
    get_dask_array_from_fits_mock.assert_called_once_with(
        "image_path", 0, (2, 3, 1, 1), ">f4"
    )
    xr.testing.assert_allclose(output_xrda, expected_xrda)
    assert output_xrda.chunks is None


@patch("ska_sdp_spectral_line_imaging.util.get_dask_array_from_fits")
@patch("ska_sdp_spectral_line_imaging.util.fits.open")
@patch("ska_sdp_spectral_line_imaging.util.WCS")
def test_should_read_fits_image_when_fits_has_no_freq_pol(
    wcs_mock, fits_open_mock, get_dask_array_from_fits_mock
):
    hud0 = MagicMock(name="fits_hud_0")
    hud0.data.shape = (2, 2)
    hud0.data.dtype = ">f4"

    wcs = MagicMock(name="wcs")
    wcs.axis_type_names = ["RA", "DEC"]

    fits_open_mock.return_value = fits_open_mock
    fits_open_mock.__enter__.return_value = [hud0]
    wcs_mock.return_value = wcs
    get_dask_array_from_fits_mock.return_value = (
        np.arange(4).reshape((2, 2)).astype(np.float32)
    )

    expected_xrda = xr.DataArray(
        np.array([[0, 1], [2, 3]], dtype=np.float32), dims=["y", "x"]
    )

    output_xrda = get_dataarray_from_fits("image_path", 0)

    fits_open_mock.assert_called_once_with("image_path", memmap=True)
    wcs_mock.assert_called_once_with("image_path")
    get_dask_array_from_fits_mock.assert_called_once_with(
        "image_path", 0, (2, 2), ">f4"
    )
    xr.testing.assert_allclose(output_xrda, expected_xrda)
    assert output_xrda.chunks is None
