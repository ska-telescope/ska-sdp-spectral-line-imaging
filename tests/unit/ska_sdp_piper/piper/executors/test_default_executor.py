from mock import mock

from ska_sdp_piper.piper.executors.default_executor import DefaultExecutor


@mock.patch("ska_sdp_piper.piper.executors.default_executor.dask.compute")
def test_should_execute_scheduled_stages(compute_mock):
    executor = DefaultExecutor()
    tasks = ["OUT_1", "OUT_2", "OUT_3"]
    executor.execute(tasks)

    compute_mock.assert_called_once_with(
        "OUT_1", "OUT_2", "OUT_3", optimize_graph=True
    )
