# pylint: disable=no-member
import pytest
from mock import mock
from mock.mock import Mock, call

from ska_sdp_spectral_line_imaging.stages.imaging import imaging_stage
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_polarization",
    return_value="polarization_frame",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_wcs", return_value="wcs"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.clean_cube")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.export_image_as")
def test_should_do_imaging_for_dirty_image_when_nmajor_iter_is_zero(
    export_image_as_mock, clean_cube_mock, get_wcs_mock, get_pol_mock
):
    clean_cube_mock.return_value = {"dirty": "dirty_image"}
    export_image_as_mock.return_value = "dirty_export_task"

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

    upstream_output = imaging_stage.stage_definition(
        upstream_output,
        gridding_params,
        {"deconvolve_params": None},
        0,
        "psf_path",
        {"bmaj": "bmaj"},
        "image_name",
        "fits",
        True,
        True,
        True,
        "output_dir",
    )

    get_pol_mock.assert_called_once_with(ps)
    get_wcs_mock.assert_called_once_with(ps, 123, 456, 456)

    clean_cube_mock.assert_called_once_with(
        ps,
        "psf_path",
        0,
        gridding_params,
        {"deconvolve_params": None},
        "polarization_frame",
        "wcs",
        {"bmaj": "bmaj"},
    )

    export_image_as_mock.assert_called_once_with(
        "dirty_image", "output_dir/image_name.dirty", "fits"
    )

    assert upstream_output.ps == ps
    assert upstream_output.compute_tasks == ["dirty_export_task"]


@pytest.mark.parametrize(
    "n_iter_major,exports,products,tasks",
    [
        (
            1,
            [True, True, True],
            ["model", "psf", "residual", "restored"],
            [
                "task_1",
                "task_2",
                "task_3",
                "task_4",
            ],
        ),
        (
            1,
            [False, True, True],
            ["psf", "residual", "restored"],
            ["task_1", "task_2", "task_3"],
        ),
        (
            1,
            [True, False, True],
            ["model", "residual", "restored"],
            ["task_1", "task_2", "task_3"],
        ),
        (
            1,
            [True, True, False],
            ["model", "psf", "restored"],
            ["task_1", "task_2", "task_3"],
        ),
    ],
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_polarization",
    return_value="polarization_frame",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_wcs", return_value="wcs"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.export_image_as")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.clean_cube")
def test_should_export_clean_artefacts(
    clean_cube_mock,
    export_image_as_mock,
    get_wcs_mock,
    get_pol_mock,
    n_iter_major,
    exports,
    products,
    tasks,
):
    clean_cube_mock.return_value = {
        "model": "model_image",
        "psf": "psf_image",
        "residual": "residual_image",
        "restored": "restored_image",
    }

    export_image_as_mock.side_effect = [
        "task_1",
        "task_2",
        "task_3",
        "task_4",
    ]

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

    imaging_stage.stage_definition(
        upstream_output,
        gridding_params,
        {"deconvolve_params": None},
        n_iter_major,
        None,
        None,
        "image_name",
        "fits",
        *exports,
        "output_dir",
    )

    get_pol_mock.assert_called_once_with(ps)
    get_wcs_mock.assert_called_once_with(ps, 123, 456, 456)

    clean_cube_mock.assert_called_once_with(
        ps,
        None,
        n_iter_major,
        gridding_params,
        {"deconvolve_params": None},
        "polarization_frame",
        "wcs",
        None,
    )

    compute_tasks = upstream_output.compute_tasks
    calls = [
        call(f"{product}_image", f"output_dir/image_name.{product}", "fits")
        for product in products
    ]
    export_image_as_mock.assert_has_calls(calls)

    assert upstream_output.ps == ps
    assert compute_tasks == tasks


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_polarization",
    return_value="polarization_frame",
)
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.imaging.get_wcs", return_value="wcs"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.estimate_cell_size")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.estimate_image_size")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.np")
@mock.patch("ska_sdp_spectral_line_imaging.stages.imaging.clean_cube")
def test_should_estimate_image_and_cell_size(
    clean_cube_mock,
    numpy_mock,
    estimate_image_size_mock,
    estimate_cell_size_mock,
    get_wcs_mock,
    get_pol_mock,
):
    max_baseline = Mock(name="max_baseline")
    numpy_mock.maximum.return_value = max_baseline
    max_baseline.round.return_value = 3.45

    cell_size_xdr = Mock(name="cell_size_dataarray")
    estimate_cell_size_mock.return_value = cell_size_xdr
    cell_size_xdr.compute.return_value = 0.75

    image_size_xdr = Mock(name="image_size_dataarray")
    estimate_image_size_mock.return_value = image_size_xdr
    image_size_xdr.compute.return_value = 500

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
        0,
        "psf_path",
        {"beam_info": "beam_info"},
        "image_name",
        "fits",
        False,
        False,
        False,
        "output_dir",
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

    clean_cube_mock.assert_called_once_with(
        ps,
        "psf_path",
        0,
        {
            "epsilon": 1e-4,
            "cell_size": 0.75,
            "image_size": 500,
            "scaling_factor": 2.0,
            "nx": 500.0,
            "ny": 500.0,
        },
        {"deconvolve_params": None},
        "polarization_frame",
        "wcs",
        {"beam_info": "beam_info"},
    )
