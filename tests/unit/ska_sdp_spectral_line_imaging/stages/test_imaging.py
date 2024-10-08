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
    max_baseline = Mock(name="max_baseline")
    numpy_mock.maximum.return_value = max_baseline
    max_baseline.round.return_value = 3.45
    estimate_cell_size_mock.return_value = 0.75
    estimate_image_size_mock.return_value = 500
    clean_cube_mock.return_value = "cube image"

    ps = Mock(name="ps")
    ps.UVW = Mock(name="UVW")
    ps.frequency.reference_frequency = {"data": 123.01}
    ps.UVW.max.return_value = ("umax", "vmax", "wmax")
    ps.frequency.min.return_value = 200
    ps.frequency.max.return_value = 400

    min_antenna_diameter = Mock(name="min_antenna_diameter")
    ps.antenna_xds.DISH_DIAMETER.min.return_value = min_antenna_diameter
    min_antenna_diameter.round.return_value = 50.5
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

    numpy_mock.abs.assert_called_once_with(ps.UVW)
    ps.UVW.max.assert_called_once_with(dim=["time", "baseline_id"])
    numpy_mock.maximum.assert_called_once_with("umax", "vmax")
    max_baseline.round.assert_called_once_with(2)

    estimate_cell_size_mock.assert_called_once_with(
        3.45,
        0.75e6,
        2.0,
    )

    min_antenna_diameter.round.assert_called_once_with(2)

    estimate_image_size_mock.assert_called_once_with(1.50e6, 50.5, 0.75)

    clean_cube_mock.assert_called_once_with(
        ps,
        "psf_image",
        0,
        {
            "epsilon": 0.0001,
            "cell_size": 0.75,
            "image_size": 500,
            "scaling_factor": 2.0,
            "nx": 500,
            "ny": 500,
        },
        {"deconvolve_params": None},
    )

    import_image_mock.assert_called_once_with("psf_path", fixpol=True)

    assert result == {"ps": ps, "image_cube": "cube image"}
