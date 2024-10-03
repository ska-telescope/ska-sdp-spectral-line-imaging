# pylint: disable=no-member
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
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.np")
def test_should_estimate_cell_size_when_not_passed(
    numpy_mock, cube_imaging_mock, estimate_cell_size_mock
):
    ps = Mock(name="ps")
    ps.UVW = Mock(name="uvw")
    ps.UVW.max.return_value = ("umax", "vmax", "wmax")
    ps.frequency.max.return_value = 123.01

    numpy_mock.abs.return_value = ps.UVW
    numpy_mock.minimum.return_value = "min_cell_size"
    estimate_cell_size_mock.side_effect = ["u_cell_size", "v_cell_size"]

    upstream_output = {"ps": ps}
    epsilon = 1e-4
    cell_size = None
    nx = 0
    ny = 1
    scaling_factor = 2.0

    imaging_stage.stage_definition(
        upstream_output, epsilon, cell_size, scaling_factor, nx, ny
    )

    numpy_mock.abs.assert_called_once_with(ps.UVW)
    ps.UVW.max.assert_called_once_with(dim=["time", "baseline_id"])
    estimate_cell_size_mock.assert_has_calls(
        [
            mock.call(
                "umax",
                123.01,
                2.0,
            ),
            mock.call(
                "vmax",
                123.01,
                2.0,
            ),
        ]
    )
    numpy_mock.minimum.assert_called_once_with("u_cell_size", "v_cell_size")
    cube_imaging_mock.assert_called_once_with(ps, "min_cell_size", 0, 1, 1e-4)
