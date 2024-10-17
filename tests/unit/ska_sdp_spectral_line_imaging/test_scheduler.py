from mock import Mock, mock

from ska_sdp_spectral_line_imaging.scheduler import DefaultScheduler


@mock.patch("ska_sdp_spectral_line_imaging.scheduler.UpstreamOutput")
def test_should_schedule_stages_with_configuration_params(
    upstream_output_mock,
):
    upstream_output_mock.return_value = upstream_output_mock

    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.stage_definition = "stage1_definition"
    stage1.get_stage_arguments.return_value = {"arg1": 1}
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.stage_definition = "stage2_definition"
    stage2.get_stage_arguments.return_value = {"arg2": 2}
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.stage_definition = "stage3_definition"
    stage3.get_stage_arguments.return_value = {"arg3": 3}

    default_scheduler = DefaultScheduler()
    default_scheduler.schedule([stage1, stage2, stage3])

    stage1.assert_called_once_with(upstream_output_mock)
    stage2.assert_called_once_with("Stage_1 output")
    stage3.assert_called_once_with("Stage_2 output")


def test_should_append_tasks():
    scheduler = DefaultScheduler()
    scheduler.append("TASK1")

    assert scheduler.tasks == ["TASK1"]


def test_should_extend_tasks():
    scheduler = DefaultScheduler()
    scheduler.extend(["TASK1", "TASK2"])

    assert scheduler.tasks == ["TASK1", "TASK2"]
