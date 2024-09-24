from dask.distributed import Client, performance_report

from .default_executor import DefaultExecutor


class DistributedExecutor(DefaultExecutor):
    """
    A distributed dask based scheduler

    Attributes
    ----------
      client: dask.distributed.Client
        The client created for scheduling and executing the dask tasks
    """

    def __init__(
        self, dask_scheduler, output_dir, with_report=False, **kwargs
    ):
        """
        Instantiate a distributed dask scheduler

        Parameters
        ----------
          dask_scheduler: str
            URL of the dask scheduler
          output_dir: str
            Path to output directory
          with_report: bool
            Execute and generate report if with_report == False
          **kwargs: dict
            Additional keyword arguments
        """
        super().__init__()
        self.client = Client(dask_scheduler)
        self.client.forward_logging()

        self.report_file = f"{output_dir}/dask_report.html"
        self.with_report = with_report

    def execute(self, tasks):
        if self.with_report:
            with performance_report(filename=self.report_file):
                return super().execute(tasks)

        return super().execute(tasks)
