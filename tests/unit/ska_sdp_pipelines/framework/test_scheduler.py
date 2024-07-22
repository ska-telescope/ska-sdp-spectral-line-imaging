from mock import Mock, mock

from ska_sdp_pipelines.framework.configuration import Configuration
from ska_sdp_pipelines.framework.log_util import LogUtil
from ska_sdp_pipelines.framework.model.config_manager import ConfigManager
from ska_sdp_pipelines.framework.scheduler import (
    DaskScheduler,
    DefaultScheduler,
    SchedulerFactory,
)


def test_should_get_default_scheduler():
    actual = SchedulerFactory.get_scheduler()
    assert isinstance(actual, DefaultScheduler)


@mock.patch("ska_sdp_pipelines.framework.scheduler.Client")
def test_should_get_dask_scheduler(client_mock):
    actual = SchedulerFactory.get_scheduler(dask_scheduler="url")
    assert isinstance(actual, DaskScheduler)


@mock.patch("ska_sdp_pipelines.framework.scheduler.dask.delayed")
def test_should_schedule_stages_with_configuration_params(
    delayed_mock,
):
    config = ConfigManager(
        {
            "pipeline": {"stage1": True, "stage2": True, "stage3": True},
            "parameters": {
                "stage1": {"stage1_parameter_1": 0},
                "stage2": {"stage2_parameter_1": 0},
                "stage3": {"stage3_parameter_1": 0},
            },
        }
    )

    delayed_mock_call_1 = Mock(name="delay1", return_value="DELAYED_1")
    delayed_mock_call_2 = Mock(name="delay2", return_value="DELAYED_2")
    delayed_mock_call_3 = Mock(name="delay3", return_value="DELAYED_3")

    delayed_mock.side_effect = [
        delayed_mock_call_1,
        delayed_mock_call_2,
        delayed_mock_call_3,
    ]
    stage1 = Mock(name="mock_stage_1", return_value="Stage_1 output")
    stage1.name = "stage1"
    stage1.stage_config = Configuration()
    stage2 = Mock(name="mock_stage_2", return_value="Stage_2 output")
    stage2.name = "stage2"
    stage2.stage_config = Configuration()
    stage3 = Mock(name="mock_stage_3", return_value="Stage_3 output")
    stage3.name = "stage3"
    stage3.stage_config = Configuration()

    default_scheduler = DefaultScheduler()
    default_scheduler.schedule(
        [stage1, stage2, stage3],
        "dataset",
        config,
        "output_dir",
        arg1="arg1",
        arg2="arg2",
    )

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
        {
            "input_data": "dataset",
            "output": None,
            "output_dir": "output_dir",
            "additional_arguments": {"arg1": "arg1", "arg2": "arg2"},
        },
        stage1_parameter_1=0,
    )
    delayed_mock_call_2.assert_called_once_with(
        False,
        stage2,
        {
            "input_data": "dataset",
            "output": "DELAYED_1",
            "output_dir": "output_dir",
            "additional_arguments": {"arg1": "arg1", "arg2": "arg2"},
        },
        stage2_parameter_1=0,
    )
    delayed_mock_call_3.assert_called_once_with(
        False,
        stage3,
        {
            "input_data": "dataset",
            "output": "DELAYED_2",
            "output_dir": "output_dir",
            "additional_arguments": {"arg1": "arg1", "arg2": "arg2"},
        },
        stage3_parameter_1=0,
    )

    assert default_scheduler.delayed_outputs == [
        "DELAYED_1",
        "DELAYED_2",
        "DELAYED_3",
    ]


@mock.patch("ska_sdp_pipelines.framework.scheduler.dask.compute")
def test_should_execute_scheduled_stages(compute_mock):
    scheduler = DefaultScheduler()
    scheduler.delayed_outputs = ["OUT_1", "OUT_2", "OUT_3"]
    scheduler.execute()

    compute_mock.assert_called_once_with("OUT_1", "OUT_2", "OUT_3")


@mock.patch("ska_sdp_pipelines.framework.scheduler.Client")
def test_should_create_dask_client_with_logging_forwarded(client_mock):
    client_mock.return_value = client_mock

    DaskScheduler(dask_scheduler="url")

    client_mock.assert_called_once_with("url")
    client_mock.forward_logging.assert_called_once()
