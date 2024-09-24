from mock import mock

from ska_sdp_piper.piper.executors.distributed_executor import (
    DistributedExecutor,
)


@mock.patch(
    "ska_sdp_piper.piper.executors.distributed_executor.performance_report"
)
@mock.patch("ska_sdp_piper.piper.executors.distributed_executor.Client")
@mock.patch("ska_sdp_piper.piper.executors.default_executor.dask.compute")
def test_should_dask_execute_scheduled_stages_with_report(
    compute_mock, client_mock, performance_report_mock
):
    executor = DistributedExecutor("url", "output_dir", with_report=True)
    tasks = ["OUT_1", "OUT_2", "OUT_3"]
    executor.execute(tasks)

    performance_report_mock.assert_called_once_with(
        filename="output_dir/dask_report.html"
    )
    compute_mock.assert_called_once_with(
        "OUT_1", "OUT_2", "OUT_3", optimize=True
    )


@mock.patch(
    "ska_sdp_piper.piper.executors.distributed_executor.performance_report"
)
@mock.patch("ska_sdp_piper.piper.executors.distributed_executor.Client")
@mock.patch("ska_sdp_piper.piper.executors.default_executor.dask.compute")
def test_should_dask_execute_scheduled_stages_without_report(
    compute_mock, client_mock, performance_report_mock
):
    executor = DistributedExecutor("url", "output_dir")
    tasks = ["OUT_1", "OUT_2", "OUT_3"]
    executor.execute(tasks)

    assert performance_report_mock.call_count == 0
    compute_mock.assert_called_once_with(
        "OUT_1", "OUT_2", "OUT_3", optimize=True
    )


@mock.patch("ska_sdp_piper.piper.executors.distributed_executor.Client")
def test_should_create_dask_client_with_logging_forwarded(client_mock):
    client_mock.return_value = client_mock

    DistributedExecutor("dask_url", "output_dir")

    client_mock.assert_called_once_with("dask_url")
    client_mock.forward_logging.assert_called_once()
