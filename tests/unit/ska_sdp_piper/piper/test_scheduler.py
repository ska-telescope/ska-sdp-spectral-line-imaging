from mock import Mock, mock

from ska_sdp_piper.piper.scheduler import (
    DaskScheduler,
    DefaultScheduler,
    SchedulerFactory,
)


def test_should_get_default_scheduler():
    actual = SchedulerFactory.get_scheduler("output_dir")
    assert isinstance(actual, DefaultScheduler)


@mock.patch("ska_sdp_piper.piper.scheduler.Client")
def test_should_get_dask_scheduler(client_mock):
    actual = SchedulerFactory.get_scheduler("output_dir", dask_scheduler="url")
    assert isinstance(actual, DaskScheduler)


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

    assert default_scheduler.delayed_outputs == [
        "DELAYED_1",
        "DELAYED_2",
        "DELAYED_3",
    ]


@mock.patch("ska_sdp_piper.piper.scheduler.Client")
@mock.patch("ska_sdp_piper.piper.scheduler.dask.delayed")
def test_should_dask_schedule_stages_with_report(delayed_mock, client_mock):
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

    default_scheduler = DaskScheduler("url", "output_dir", with_report=1)
    default_scheduler.schedule([stage1, stage2, stage3])

    delayed_mock.assert_has_calls(
        [
            mock.call(stage1),
            mock.call(stage2),
            mock.call(stage3),
        ]
    )

    delayed_mock_call_1.assert_called_once_with(None, False)
    delayed_mock_call_2.assert_called_once_with("DELAYED_1", False)
    delayed_mock_call_3.assert_called_once_with("DELAYED_2", False)

    assert default_scheduler.delayed_outputs == [
        "DELAYED_1",
        "DELAYED_2",
        "DELAYED_3",
    ]


@mock.patch("ska_sdp_piper.piper.scheduler.Client")
@mock.patch("ska_sdp_piper.piper.scheduler.dask.delayed")
def test_should_dask_schedule_stages_with_configuration_params(
    delayed_mock, client_mock
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

    default_scheduler = DaskScheduler("url", "output_dir", with_report=0)
    default_scheduler.schedule([stage1, stage2, stage3])

    delayed_mock.assert_has_calls(
        [
            mock.call(stage1),
            mock.call(stage2),
            mock.call(stage3),
        ]
    )


@mock.patch("ska_sdp_piper.piper.scheduler.dask.compute")
def test_should_execute_scheduled_stages(compute_mock):
    scheduler = DefaultScheduler()
    scheduler.delayed_outputs = ["OUT_1", "OUT_2", "OUT_3"]
    scheduler.execute()

    compute_mock.assert_called_once_with(
        "OUT_1", "OUT_2", "OUT_3", optimize=True
    )


@mock.patch("ska_sdp_piper.piper.scheduler.performance_report")
@mock.patch("ska_sdp_piper.piper.scheduler.Client")
@mock.patch("ska_sdp_piper.piper.scheduler.dask.compute")
def test_should_dask_execute_scheduled_stages_with_report(
    compute_mock, client_mock, performance_report_mock
):
    scheduler = DaskScheduler("url", "output_dir", with_report=True)
    scheduler.delayed_outputs = ["OUT_1", "OUT_2", "OUT_3"]
    scheduler.execute()

    performance_report_mock.assert_called_once_with(
        filename="output_dir/dask_report.html"
    )
    compute_mock.assert_called_once_with(
        "OUT_1", "OUT_2", "OUT_3", optimize=True
    )


@mock.patch("ska_sdp_piper.piper.scheduler.performance_report")
@mock.patch("ska_sdp_piper.piper.scheduler.Client")
@mock.patch("ska_sdp_piper.piper.scheduler.dask.compute")
def test_should_dask_execute_scheduled_stages_without_report(
    compute_mock, client_mock, performance_report_mock
):
    scheduler = DaskScheduler("url", "output_dir")
    scheduler.delayed_outputs = ["OUT_1", "OUT_2", "OUT_3"]
    scheduler.execute()

    assert performance_report_mock.call_count == 0
    compute_mock.assert_called_once_with(
        "OUT_1", "OUT_2", "OUT_3", optimize=True
    )


@mock.patch("ska_sdp_piper.piper.scheduler.Client")
def test_should_create_dask_client_with_logging_forwarded(client_mock):
    client_mock.return_value = client_mock

    DaskScheduler("dask_url", "output_dir")

    client_mock.assert_called_once_with("dask_url")
    client_mock.forward_logging.assert_called_once()
