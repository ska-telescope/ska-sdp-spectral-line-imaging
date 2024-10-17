# pylint: disable=no-member
from mock import mock
from mock.mock import Mock

from ska_sdp_spectral_line_imaging.stages.imaging import imaging_stage
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_polarization",
    return_value="polarization_frame",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_wcs", return_value="wcs"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.cube_imaging")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.clean_cube")
def test_should_do_imaging_for_dirty_image(
    clean_cube_mock, cube_imaging_mock, get_wcs_mock, get_pol_mock
):
    cube_imaging_mock.return_value = "dirty image"

    ps = Mock(name="ps")
    ps.UVW = "UVW"
    ps.frequency.reference_frequency = {"data": 123.01}
    gridding_params = {
        "epsilon": 1e-4,
        "cell_size": 123,
        "image_size": 456,
        "scaling_factor": 2.0,
    }

    upstream_output = UpstreamOutput()
    upstream_output["ps"] = ps

    do_clean = False

    imaging_stage.stage_definition(
        upstream_output,
        gridding_params,
        {"deconvolve_params": None},
        do_clean,
        0,
        "psf_path",
        {"beam_info": "beam_info"},
    )

    get_pol_mock.assert_called_once_with(ps)
    get_wcs_mock.assert_called_once_with(ps, 123, 456, 456)

    cube_imaging_mock.assert_called_once_with(
        ps, 123, 456, 456, 1e-4, "wcs", "polarization_frame"
    )

    clean_cube_mock.assert_not_called()

    assert upstream_output.ps == ps
    assert upstream_output.image_cube == "dirty image"


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_polarization",
    return_value="polarization_frame",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_wcs", return_value="wcs"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.cube_imaging")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.clean_cube")
def test_should_do_imaging_for_clean_image(
    clean_cube_mock, cube_imaging_mock, get_wcs_mock, get_pol_mock
):
    cube_imaging_mock.return_value = "dirty image"
    clean_cube_mock.return_value = ("restored image", "residual image")

    ps = Mock(name="ps")
    ps.UVW = "UVW"
    ps.frequency.reference_frequency = {"data": 123.01}
    gridding_params = {
        "epsilon": 1e-4,
        "cell_size": 123,
        "image_size": 456,
        "scaling_factor": 2.0,
    }

    upstream_output = UpstreamOutput()
    upstream_output["ps"] = ps
    do_clean = True

    imaging_stage.stage_definition(
        upstream_output,
        gridding_params,
        {"deconvolve_params": None},
        do_clean,
        15,
        "psf_path",
        {"beam_info": "beam_info"},
    )

    get_pol_mock.assert_called_once_with(ps)
    get_wcs_mock.assert_called_once_with(ps, 123, 456, 456)

    cube_imaging_mock.assert_called_once_with(
        ps, 123, 456, 456, 1e-4, "wcs", "polarization_frame"
    )

    clean_cube_mock.assert_called_once_with(
        ps,
        "psf_path",
        "dirty image",
        15,
        {
            "epsilon": 1e-4,
            "cell_size": 123,
            "image_size": 456,
            "scaling_factor": 2.0,
            "nx": 456,
            "ny": 456,
        },
        {"deconvolve_params": None},
        "polarization_frame",
        "wcs",
        {"beam_info": "beam_info"},
    )

    assert upstream_output.ps == ps
    assert upstream_output.image_cube == "restored image"


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_polarization",
    return_value="polarization_frame",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_wcs", return_value="wcs"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.cube_imaging")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.estimate_cell_size")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.estimate_image_size")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.np")
def test_should_estimate_image_and_cell_size(
    numpy_mock,
    estimate_image_size_mock,
    estimate_cell_size_mock,
    cube_imaging_mock,
    get_wcs_mock,
    get_pol_mock,
):
    max_baseline = Mock(name="max_baseline")
    numpy_mock.maximum.return_value = max_baseline
    max_baseline.round.return_value = 3.45
    estimate_cell_size_mock.return_value = 0.75
    estimate_image_size_mock.return_value = 500
    cube_imaging_mock.return_value = "dirty image"

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

    upstream_output = UpstreamOutput()
    upstream_output["ps"] = ps

    imaging_stage.stage_definition(
        upstream_output,
        gridding_params,
        {"deconvolve_params": None},
        False,
        0,
        "psf_path",
        {"beam_info": "beam_info"},
    )

    numpy_mock.abs.assert_called_once_with(ps.UVW)
    ps.UVW.max.assert_called_once_with(dim=["time", "baseline_id"])
    numpy_mock.maximum.assert_called_once_with("umax", "vmax")
    max_baseline.round.assert_called_once_with(2)

    estimate_cell_size_mock.assert_called_once_with(
        3.45,
        749481.145,
        2.0,
    )

    min_antenna_diameter.round.assert_called_once_with(2)

    estimate_image_size_mock.assert_called_once_with(1498962.29, 50.5, 0.75)

    get_pol_mock.assert_called_once_with(ps)
    get_wcs_mock.assert_called_once_with(ps, 0.75, 500, 500)

    cube_imaging_mock.assert_called_once_with(
        ps, 0.75, 500, 500, 1e-4, "wcs", "polarization_frame"
    )

    assert upstream_output.ps == ps
    assert upstream_output.image_cube == "dirty image"
