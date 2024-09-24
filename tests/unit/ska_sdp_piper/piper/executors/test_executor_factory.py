from mock import mock

from ska_sdp_piper.piper.executors import ExecutorFactory


@mock.patch("ska_sdp_piper.piper.executors.executor_factory.DefaultExecutor")
def test_should_get_default_executor(default_executor_mock):
    ExecutorFactory.get_executor("output_dir")
    default_executor_mock.assert_called_once()


@mock.patch(
    "ska_sdp_piper.piper.executors.executor_factory.DistributedExecutor"
)
def test_should_get_dask_executor(distributed_executor_mock):
    ExecutorFactory.get_executor("output_dir", dask_scheduler="url")
    distributed_executor_mock.assert_called_once_with("url", "output_dir")
