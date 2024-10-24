# pylint: disable=no-member
from mock import Mock, mock

from ska_sdp_spectral_line_imaging.stages.data_export import (
    export_image,
    export_model,
    export_residual,
)
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


def test_should_export_residual():

    observation = Mock(name="observation")
    observation.VISIBILITY.to_zarr = Mock(
        name="to_zarr", return_value="export_task"
    )
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation

    export_residual.stage_definition(upstream_output, "residual", "output_dir")

    observation.VISIBILITY.attrs.clear.assert_called_once()

    observation.VISIBILITY.to_zarr.assert_called_once_with(
        store="output_dir/residual.zarr", compute=False
    )

    assert upstream_output.compute_tasks == ["export_task"]


def test_should_export_model():

    observation = Mock(name="observation")
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    observation.VISIBILITY_MODEL.to_zarr = Mock(
        name="to_zarr", return_value="export_task"
    )

    export_model.stage_definition(upstream_output, "model", "output_dir")

    observation.VISIBILITY_MODEL.attrs.clear.assert_called_once()

    observation.VISIBILITY_MODEL.to_zarr.assert_called_once_with(
        store="output_dir/model.zarr", compute=False
    )

    assert upstream_output.compute_tasks == ["export_task"]


@mock.patch("ska_sdp_spectral_line_imaging.stages.data_export.export_data_as")
def test_should_export_fits(mock_export_data_as):
    cube = Mock(name="cube_data")
    upstream_output = UpstreamOutput()
    upstream_output["image_cube"] = cube
    mock_export_data_as.return_value = "fits_export"

    export_image.stage_definition(
        upstream_output,
        {"image_name": "image_name", "export_format": "fits"},
        "output_dir",
    )

    mock_export_data_as.assert_called_once_with(
        cube, "output_dir/image_name", "fits"
    )

    assert len(upstream_output.compute_tasks) == 1


@mock.patch("ska_sdp_spectral_line_imaging.stages.data_export.export_data_as")
def test_should_export_zarr(mock_export_data_as):

    cube = Mock(name="cube_data")
    upstream_output = UpstreamOutput()
    upstream_output["image_cube"] = cube
    mock_export_data_as.return_value = "export_zarr"

    export_image.stage_definition(
        upstream_output,
        {"image_name": "image_name", "export_format": "zarr"},
        "output_dir",
    )

    mock_export_data_as.assert_called_once_with(
        cube, "output_dir/image_name", "zarr"
    )

    assert upstream_output.compute_tasks == ["export_zarr"]
