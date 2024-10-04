# pylint: disable=no-member
from mock import mock
from mock.mock import Mock

from ska_sdp_spectral_line_imaging.stages.imaging import imaging_stage


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.import_image_from_fits",
    return_value="psf_image",
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.clean_cube")
def test_should_do_imaging(clean_cube_mock, import_image_mock):
    clean_cube_mock.return_value = "cube image"

    ps = Mock(name="ps")
    ps.UVW = "UVW"
    ps.frequency.reference_frequency = {"data": 123.01}

    gridding_params = {
        "epsilon": 1e-4,
        "cell_size": 123,
        "image_size": 1,
        "scaling_factor": 2.0,
    }

    upstream_output = {"ps": ps}

    result = imaging_stage.stage_definition(
        upstream_output,
        gridding_params,
        {"deconvolve_params": None},
        0,
        "psf_path",
    )

    clean_cube_mock.assert_called_once_with(
        ps,
        "psf_image",
        0,
        {
            "epsilon": 0.0001,
            "cell_size": 123,
            "image_size": 1,
            "scaling_factor": 2.0,
            "nx": 1,
            "ny": 1,
        },
        {"deconvolve_params": None},
    )

    import_image_mock.assert_called_once_with("psf_path", fixpol=True)

    assert result == {"ps": ps, "image_cube": "cube image"}


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.import_image_from_fits",
    return_value="psf_image",
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.clean_cube")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.estimate_cell_size")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.estimate_image_size")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.np")
def test_should_estimate_image_and_cell_size(
    numpy_mock,
    estimate_image_size_mock,
    estimate_cell_size_mock,
    clean_cube_mock,
    import_image_mock,
):
    clean_cube_mock.return_value = "cube image"
    estimate_cell_size_mock.side_effect = ["u_cell_size", "v_cell_size"]
    estimate_image_size_mock.return_value = 73

    numpy_mock.minimum.return_value = 12.23

    ps = Mock(name="ps")
    ps.UVW = Mock(name="UVW")
    ps.frequency.reference_frequency = {"data": 123.01}
    ps.UVW.max.return_value = ("umax", "vmax", "wmax")
    ps.frequency.min.return_value = 200
    ps.frequency.max.return_value = 400
    ps.antenna_xds.DISH_DIAMETER.min.return_value = 50
    numpy_mock.abs.return_value = ps.UVW

    gridding_params = {
        "epsilon": 1e-4,
        "cell_size": None,
        "image_size": None,
        "scaling_factor": 2.0,
    }

    upstream_output = {"ps": ps}

    result = imaging_stage.stage_definition(
        upstream_output,
        gridding_params,
        {"deconvolve_params": None},
        0,
        "psf_path",
    )

    estimate_cell_size_mock.assert_has_calls(
        [
            mock.call(
                "umax",
                0.75e6,
                2.0,
            ),
            mock.call(
                "vmax",
                0.75e6,
                2.0,
            ),
        ]
    )

    estimate_image_size_mock.assert_called_once_with(1.50e6, 50, 12.23)

    clean_cube_mock.assert_called_once_with(
        ps,
        "psf_image",
        0,
        {
            "epsilon": 0.0001,
            "cell_size": 12.23,
            "image_size": 73,
            "scaling_factor": 2.0,
            "nx": 73,
            "ny": 73,
        },
        {"deconvolve_params": None},
    )

    numpy_mock.minimum.assert_called_once_with("u_cell_size", "v_cell_size")

    numpy_mock.abs.assert_called_once_with(ps.UVW)

    ps.UVW.max.assert_called_once_with(dim=["time", "baseline_id"])

    import_image_mock.assert_called_once_with("psf_path", fixpol=True)

    assert result == {"ps": ps, "image_cube": "cube image"}
