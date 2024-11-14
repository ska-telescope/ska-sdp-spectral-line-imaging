# pylint: disable=no-member
import mock
import numpy as np
import xarray as xr
from mock import MagicMock, Mock, call

from ska_sdp_spectral_line_imaging.stages.model import cont_sub, read_model
from ska_sdp_spectral_line_imaging.upstream_output import UpstreamOutput


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.report_peak_visibility"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
def test_should_perform_continuum_subtraction(
    np_mock, report_peak_mock, subtract_visibility_mock
):

    observation = Mock(name="observation")
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    observation.assign.return_value = "model"
    subtracted_vis = Mock(name="subtracted_vis")
    subtracted_vis.VISIBILITY.assign_attrs.return_value = "sub_vis_with_attrs"
    subtracted_vis.frequency = Mock(name="frequency")
    subtracted_vis.frequency.units = ["Hz"]
    subtracted_vis.assign.return_value = subtracted_vis
    subtract_visibility_mock.return_value = subtracted_vis

    output = cont_sub.stage_definition(
        upstream_output, False, "ps_out", False, "output_path"
    )

    observation.assign.assert_called_once_with(
        {"VISIBILITY": observation.VISIBILITY_MODEL}
    )
    subtract_visibility_mock.assert_called_once_with(observation, "model")
    subtracted_vis.assign.assert_called_once_with(
        {"VISIBILITY": "sub_vis_with_attrs"}
    )

    report_peak_mock.assert_called_once_with(subtracted_vis.VISIBILITY, "Hz")
    assert output.ps == subtracted_vis.copy()


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.logger")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.delayed_log")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.report_peak_visibility"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.fit_polynomial_on_visibility"
)
def test_should_report_extent_of_continuum_subtraction(
    fit_poly_on_vis_mock,
    numpy_mock,
    report_peak_mock,
    subtract_visibility_mock,
    delayed_log_mock,
    logger_mock,
):
    fit_poly_on_vis_mock.side_effect = [
        ["fit_real0", "fit_real1"],
        ["fit_imag0", "fit_imag1"],
    ]

    observation = MagicMock(name="observation")
    observation.polarization.values = ["RR", "LL"]
    flagged_vis = MagicMock(name="flagged_vis")
    observation.VISIBILITY.where.return_value = flagged_vis
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    subtract_visibility_mock.return_value = observation
    observation.assign.return_value = observation

    cont_sub.stage_definition(
        upstream_output, False, "ps_out", True, "output_path"
    )

    # pylint: disable=no-member
    observation.VISIBILITY.where.assert_called_once_with(
        numpy_mock.logical_not(observation.FLAG)
    )

    fit_poly_on_vis_mock.assert_has_calls(
        [
            call(flagged_vis.real),
            call(flagged_vis.imag),
        ]
    )

    delayed_log_mock.assert_has_calls(
        [
            call(
                logger_mock.info,
                "Slope of fit on real part {fit}",
                fit="fit_real1",
            ),
            call(
                logger_mock.info,
                "Slope of fit on imag part {fit}",
                fit="fit_imag1",
            ),
        ]
    )


@mock.patch("ska_sdp_spectral_line_imaging.stages.model.logger")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.report_peak_visibility"
)
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.subtract_visibility")
@mock.patch("ska_sdp_spectral_line_imaging.stages.model.np")
@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.fit_polynomial_on_visibility"
)
def test_should_not_report_extent_of_continuum_subtraction_for_invalid_pols(
    fit_poly_on_vis_mock,
    numpy_mock,
    subtract_visibility_mock,
    peak_log_mock,
    logger_mock,
):
    fit_poly_on_vis_mock.side_effect = ["fit_real", "fit_imag"]

    observation = MagicMock(name="observation")
    observation.polarization.values = ["RR", "LR"]
    upstream_output = UpstreamOutput()
    upstream_output["ps"] = observation
    subtract_visibility_mock.return_value = observation

    observation.frequency = Mock(name="frequency")
    observation.frequency.units = ["Hz"]
    observation.assign.return_value = observation

    cont_sub.stage_definition(
        upstream_output, False, "ps_out", True, "output_path"
    )

    logger_mock.warning.assert_called_once_with(
        "Cannot report extent of continuum subtraction."
    )
    fit_poly_on_vis_mock.assert_not_called()
    peak_log_mock.assert_called_once_with(observation.VISIBILITY, "Hz")


@mock.patch(
    "ska_sdp_spectral_line_imaging.stages.model.get_dataarray_from_fits"
)
def test_should_read_model_when_fits_only_has_ra_dec_freq(
    get_data_from_fits_mock,
):
    ps = MagicMock(name="ps")
    pols = ["I", "V"]
    ps.polarization.values = pols

    upout = UpstreamOutput()
    upout["ps"] = ps

    fits_image = xr.DataArray(
        data=np.arange(4).reshape((1, 2, 2)).astype(np.float32),
        dims=["frequency", "y", "x"],
        coords={},
        name="cont_fits_image",
    )

    get_data_from_fits_mock.return_value = fits_image

    expected_dataarray = xr.DataArray(
        np.array([[[0, 1], [2, 3]], [[0, 1], [2, 3]]], dtype=np.float32),
        dims=["polarization", "y", "x"],
        coords={"polarization": pols},
    ).chunk()

    output = read_model.stage_definition(
        upout,
        "test-%s-image.fits",
        do_power_law_scaling=False,
        spectral_index=0.1,
    )

    get_data_from_fits_mock.assert_has_calls(
        [mock.call("test-I-image.fits"), mock.call("test-V-image.fits")]
    )

    xr.testing.assert_allclose(expected_dataarray, output["model_image"])

    assert expected_dataarray.chunks == output["model_image"].chunks
