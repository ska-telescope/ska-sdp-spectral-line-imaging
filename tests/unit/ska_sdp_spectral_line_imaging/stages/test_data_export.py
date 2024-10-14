# pylint: disable=no-member
import logging

from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stages.data_export import (
    export_image,
    export_model,
    export_residual,
)


def test_should_export_residual():

    observation = Mock(name="observation")

    upstream_out = export_residual.stage_definition(
        {"ps": observation}, "residual", "output_dir"
    )

    observation.VISIBILITY.attrs.clear.assert_called_once()

    observation.VISIBILITY.to_zarr.assert_called_once_with(
        store="output_dir/residual.zarr"
    )

    assert upstream_out == {"ps": observation}


def test_should_export_model():

    observation = Mock(name="observation")

    upstream_out = export_model.stage_definition(
        {"ps": observation}, "model", "output_dir"
    )

    observation.VISIBILITY_MODEL.attrs.clear.assert_called_once()

    observation.VISIBILITY_MODEL.to_zarr.assert_called_once_with(
        store="output_dir/model.zarr"
    )

    assert upstream_out == {"ps": observation}


@mock.patch("ska_sdp_spectral_line_imaging.stages.data_export.fits")
def test_should_export_fits(mock_fits):

    cube = Mock(name="cube_data")
    hdu_mock = Mock(name="hdu")
    mock_fits.PrimaryHDU.return_value = hdu_mock

    upstream_out = export_image.stage_definition(
        {"image_cube": cube}, "image_name", "output_dir"
    )

    mock_fits.PrimaryHDU.assert_called_once_with(
        data=cube.pixels, header=cube.image_acc.wcs.to_header()
    )

    hdu_mock.writeto.assert_called_once_with("output_dir/image_name.fits")

    cube.to_zarr.assert_not_called()

    assert upstream_out == {"image_cube": cube}


# TODO: Revisit this once the image model pitch proceeds
@mock.patch("ska_sdp_spectral_line_imaging.stages.data_export.fits")
def test_should_export_zarr_in_case_of_HDU_exceptions(mock_fits, caplog):

    cube = Mock(name="cube_data")
    mock_fits.PrimaryHDU.side_effect = Exception()

    with caplog.at_level(logging.INFO):
        export_image.stage_definition(
            {"image_cube": cube}, "image_name", "output_dir"
        )

    mock_fits.PrimaryHDU.assert_called_once_with(
        data=cube.pixels, header=cube.image_acc.wcs.to_header()
    )

    assert caplog.records[0].levelname == "ERROR"

    # cube.to_zarr.assert_called_once_with(
    #     store="output_dir/image_name.zarr", compute=False
    # )

    # assert upstream_out == {"image_cube": cube}


# @mock.patch("ska_sdp_spectral_line_imaging.stages.data_export.fits")
# def test_should_export_zarr_in_case_of_io_exceptions(mock_fits, caplog):

#     cube = Mock(name="cube_data")
#     hdu_mock = Mock(name="hdu")
#     hdu_mock.writeto.side_effect = Exception("Write exception")
#     mock_fits.PrimaryHDU.return_value = hdu_mock

#     with caplog.at_level(logging.INFO):
#         upstream_out = export_image.stage_definition(
#             {"image_cube": cube}, "image_name", "output_dir"
#         )

#     mock_fits.PrimaryHDU.assert_called_once_with(
#         data=cube.pixels, header=cube.image_acc.wcs.to_header()
#     )

#     hdu_mock.writeto.assert_called_once_with("output_dir/image_name.fits")

#     assert caplog.records[0].levelname == "ERROR"
#     assert (
#         caplog.records[1].message == "Exporting to FITS failed. "
#         "Writing image in zarr format to path output_dir/image_name.zarr"
#     )

#     cube.to_zarr.assert_called_once_with(
#         store="output_dir/image_name.zarr", compute=False
#     )

#     assert upstream_out == {"image_cube": cube}
