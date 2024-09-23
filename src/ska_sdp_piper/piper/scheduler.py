import dask
from dask.distributed import Client, performance_report


class SchedulerFactory:
    """
    Select an appropriate scheduler based on conditions
    """

    @staticmethod
    def get_scheduler(output_dir, dask_scheduler=None, **kwargs):
        """
        Returns the approriate scheduler based on condition

        Parameters
        ----------
          dask_scheduler: str
            URL of the dask scheduler
          output_dir: str
            Path to output directory
          **kwargs: dict
            Additional keyword arguments

        Returns
        -------
           :py:class:`DefaultScheduler`
           :py:class:`DaskScheduler`
        """
        if dask_scheduler:
            return DaskScheduler(dask_scheduler, output_dir, **kwargs)

        return DefaultScheduler(**kwargs)


class DefaultScheduler:
    """
    Schedules and executes dask wrapped functions on the local machine

    Attributes
    ----------
      delayed_outputs: [dask.delayed]
        Dask delayed outputs from the scheduled tasks
    """

    def __init__(self, **kwargs):
        self.delayed_outputs = []

    def schedule(self, stages, verbose=False):
        """
        Schedules the stages as dask delayed objects

        Parameters
        ----------
          stages: list[stages.Stage]
            List of stages to schedule
          verbose: bool
            Log debug statements
        """
        output = None
        for stage in stages:
            output = dask.delayed(stage)(output, verbose)

            self.delayed_outputs.append(output)

    def append(self, task):
        self.delayed_outputs.append(task)

    def extend(self, tasks):
        self.delayed_outputs.extend(tasks)

    def execute(self):
        """
        Executes the scheduled stages.
        Since the default scheduler is dask based, the execute calls the
        compute on the scheduled dask graph
        """
        return dask.compute(*self.delayed_outputs, optimize=True)


class DaskScheduler(DefaultScheduler):
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

    def execute(self):
        if self.with_report:
            with performance_report(filename=self.report_file):
                return super().execute()

        return super().execute()
