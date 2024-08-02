from mock import Mock, mock

from ska_sdp_piper.framework.log_util import LogUtil
from ska_sdp_piper.framework.scheduler import (
    DaskScheduler,
    DefaultScheduler,
    SchedulerFactory,
)


def test_should_get_default_scheduler():
    actual = SchedulerFactory.get_scheduler()
    assert isinstance(actual, DefaultScheduler)


@mock.patch("ska_sdp_piper.framework.scheduler.Client")
def test_should_get_dask_scheduler(client_mock):
    actual = SchedulerFactory.get_scheduler(dask_scheduler="url")
    assert isinstance(actual, DaskScheduler)


@mock.patch("ska_sdp_piper.framework.scheduler.dask.delayed")
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
            mock.call(LogUtil.with_log),
            mock.call(LogUtil.with_log),
            mock.call(LogUtil.with_log),
        ]
    )

    delayed_mock_call_1.assert_called_once_with(
        False,
        stage1,
        None,
    )
    delayed_mock_call_2.assert_called_once_with(False, stage2, "DELAYED_1")
    delayed_mock_call_3.assert_called_once_with(
        False,
        stage3,
        "DELAYED_2",
    )

    assert default_scheduler.delayed_outputs == [
        "DELAYED_1",
        "DELAYED_2",
        "DELAYED_3",
    ]


@mock.patch("ska_sdp_piper.framework.scheduler.dask.compute")
def test_should_execute_scheduled_stages(compute_mock):
    scheduler = DefaultScheduler()
    scheduler.delayed_outputs = ["OUT_1", "OUT_2", "OUT_3"]
    scheduler.execute()

    compute_mock.assert_called_once_with("OUT_1", "OUT_2", "OUT_3")


@mock.patch("ska_sdp_piper.framework.scheduler.Client")
def test_should_create_dask_client_with_logging_forwarded(client_mock):
    client_mock.return_value = client_mock

    DaskScheduler(dask_scheduler="url")

    client_mock.assert_called_once_with("url")
    client_mock.forward_logging.assert_called_once()
