from mock import mock
from mock.mock import Mock

from ska_sdp_spectral_line_imaging.stages.imaging import imaging_stage


@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.cube_imaging")
def test_should_do_imaging(cube_imaging_mock):
    cube_imaging_mock.return_value = "cube image"

    ps = Mock(name="ps")
    ps.UVW = "UVW"
    ps.frequency.reference_frequency = {"data": 123.01}

    upstream_output = {"ps": ps}
    epsilon = 1e-4
    cell_size = 123
    nx = 0
    ny = 1
    scaling_factor = 2.0

    result = imaging_stage.stage_definition(
        upstream_output, epsilon, cell_size, scaling_factor, nx, ny
    )

    cube_imaging_mock.assert_called_once_with(ps, 123, 0, 1, 1e-4)

    assert result == {"ps": ps, "image_cube": "cube image"}


@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.estimate_cell_size")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.cube_imaging")
def test_should_estimate_cell_size_when_not_passed(
    cube_imaging_mock, estimate_cell_size_mock
):
    estimate_cell_size_mock.return_value = 2345
    ps = Mock(name="ps")
    ps.UVW = "UVW"
    ps.frequency.reference_frequency = {"data": 123.01}

    upstream_output = {"ps": ps}
    epsilon = 1e-4
    cell_size = None
    nx = 0
    ny = 1
    scaling_factor = 2.0

    imaging_stage.stage_definition(
        upstream_output, epsilon, cell_size, scaling_factor, nx, ny
    )

    estimate_cell_size_mock.assert_called_once_with(
        "UVW",
        123.01,
        0,
        1,
        2.0,
    )

    cube_imaging_mock.assert_called_once_with(ps, 2345, 0, 1, 1e-4)
