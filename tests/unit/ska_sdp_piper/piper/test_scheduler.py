from mock import Mock, mock

from ska_sdp_piper.piper.scheduler import DefaultScheduler


@mock.patch("ska_sdp_piper.piper.scheduler.dask.delayed")
def test_should_schedule_stages_with_configuration_params(
    delayed_mock,
):
    delayed_mock_call_1 = Mock(name="delay1", return_value="DELAYED_1")
    delayed_mock_call_2 = Mock(name="delay2", return_value="DELAYED_2")
    delayed_mock_call_3 = Mock(name="delay3", return_value="DELAYED_3")

    delayed_mock.side_effect = [
        delayed_mock_call_1,
        delayed_mock_call_2,
        delayed_mock_call_3,
    ]
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

    delayed_mock.assert_has_calls(
        [
            mock.call(stage1),
            mock.call(stage2),
            mock.call(stage3),
        ]
    )

    delayed_mock_call_1.assert_called_once_with(
        None,
        False,
    )
    delayed_mock_call_2.assert_called_once_with("DELAYED_1", False)
    delayed_mock_call_3.assert_called_once_with(
        "DELAYED_2",
        False,
    )

    assert default_scheduler.tasks == [
        "DELAYED_1",
        "DELAYED_2",
        "DELAYED_3",
    ]


def test_should_append_tasks():
    scheduler = DefaultScheduler()
    scheduler.append("TASK1")

    assert scheduler.tasks == ["TASK1"]


def test_should_extend_tasks():
    scheduler = DefaultScheduler()
    scheduler.extend(["TASK1", "TASK2"])

    assert scheduler.tasks == ["TASK1", "TASK2"]
