from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stages.data_export import export_image


@mock.patch("ska_sdp_spectral_line_imaging.stages.data_export.fits")
def test_should_export_fits(mock_fits):

    cube = Mock(name="cube data")
    hdu_mock = Mock(name="hdu")

    mock_fits.PrimaryHDU.return_value = hdu_mock

    export_image.stage_definition(
        {"image_cube": cube}, "image_name", "output_dir"
    )

    mock_fits.PrimaryHDU.assert_called_once_with(
        data=cube.pixels, header=cube.image_acc.wcs.to_header()
    )

    hdu_mock.writeto.assert_called_once_with("output_dir/image_name.fits")


@mock.patch("ska_sdp_spectral_line_imaging.stages.data_export.fits")
def test_should_export_zarr_in_case_of_HDU_exceptions(mock_fits):

    cube = Mock(name="cube data")

    mock_fits.PrimaryHDU.side_effect = Exception()

    export_image.stage_definition(
        {"image_cube": cube}, "image_name", "output_dir"
    )

    mock_fits.PrimaryHDU.assert_called_once_with(
        data=cube.pixels, header=cube.image_acc.wcs.to_header()
    )

    cube.to_zarr.assert_called_once_with(store="output_dir/image_name.zarr")


@mock.patch("ska_sdp_spectral_line_imaging.stages.data_export.fits")
def test_should_export_zarr_in_case_of_io_exceptions(mock_fits):

    cube = Mock(name="cube data")
    hdu_mock = Mock(name="hdu")
    hdu_mock.writeto.side_effect = Exception("Write exception")
    mock_fits.PrimaryHDU.return_value = hdu_mock

    export_image.stage_definition(
        {"image_cube": cube}, "image_name", "output_dir"
    )

    mock_fits.PrimaryHDU.assert_called_once_with(
        data=cube.pixels, header=cube.image_acc.wcs.to_header()
    )

    hdu_mock.writeto.assert_called_once_with("output_dir/image_name.fits")

    cube.to_zarr.assert_called_once_with(store="output_dir/image_name.zarr")
