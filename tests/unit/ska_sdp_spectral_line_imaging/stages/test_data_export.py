# pylint: disable=no-member
import pytest
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


@mock.patch("ska_sdp_spectral_line_imaging.stages.data_export.export_to_fits")
def test_should_export_fits(mock_export_fits):
    cube = Mock(name="cube_data")
    upstream_output = UpstreamOutput()
    upstream_output["image_cube"] = cube
    mock_export_fits.return_value = "fits_export"

    export_image.stage_definition(
        upstream_output, "image_name", "fits", "output_dir"
    )

    mock_export_fits.assert_called_once_with(cube, "output_dir/image_name")

    cube.to_zarr.assert_not_called()
    assert len(upstream_output.compute_tasks) == 1


def test_should_export_zarr():

    cube = Mock(name="cube_data")
    cube.to_zarr = Mock(name="to_zarr", return_value="export_zarr")
    upstream_output = UpstreamOutput()
    upstream_output["image_cube"] = cube

    export_image.stage_definition(
        upstream_output, "image_name", "zarr", "output_dir"
    )

    cube.to_zarr.assert_called_once_with(
        store="output_dir/image_name.zarr", compute=False
    )

    assert upstream_output.compute_tasks == ["export_zarr"]


def test_should_throw_exception_for_unsupported_data_format():

    cube = Mock(name="cube_data")
    cube.to_zarr = Mock(name="to_zarr", return_value="export_zarr")
    upstream_output = UpstreamOutput()
    upstream_output["image_cube"] = cube

    with pytest.raises(ValueError):
        export_image.stage_definition(
            upstream_output, "image_name", "non_supported_format", "output_dir"
        )
