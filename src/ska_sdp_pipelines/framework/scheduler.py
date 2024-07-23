import dask
from dask.distributed import Client

from .log_util import LogUtil


class SchedulerFactory:
    """
    Select an appropriate scheduler based on conditions
    """

    @staticmethod
    def get_scheduler(dask_scheduler=None):
        """
        Returns the approriate scheduler based on condition
        Parameters
        ----------
          dask_scheduler: str
            URL of the dask scheduler
        Returns
        -------
           :py:class:`DefaultScheduler`
           :py:class:`DaskScheduler`
        """
        if dask_scheduler:
            return DaskScheduler(dask_scheduler)

        return DefaultScheduler()


class DefaultScheduler:
    """
    Schedules and executes dask wrapped functions on the local machine
    Attributes
    ----------
      delayed_outputs: [dask.delayed]
        Dask delayed outputs from the scheduled tasks
    """

    def __init__(self):
        self.delayed_outputs = []

    def schedule(self, stages, verbose=False):
        """
        Schedules the stages as dask delayed objects
        Parameters
        ----------
          stages: [function]
            List of stages to schedule
          vis: Any
            Input data to be prcessed
          config: dict
            Pipeline configuration
          output_dir: str
            Output directory to store generated products
          verbose: bool
            Log debug statements
          **kwargs:
            Additional Key value args
        """
        output = None
        for stage in stages:
            output = dask.delayed(LogUtil.with_log)(
                verbose,
                stage.stage_definition,
                output,
                **stage.get_stage_arguments()
            )

            self.delayed_outputs.append(output)

    def execute(self):
        """
        Executes the scheduled stages.
        Since the default scheduler is dask based, the execute calls the
        compute on the scheduled dask graph
        """
        return dask.compute(*self.delayed_outputs)


class DaskScheduler(DefaultScheduler):
    """
    A distributed dask based scheduler
    Attributes
    ----------
      client: dask.distributed.Client
        The client created for scheduling and executing the dask tasks
    """

    def __init__(self, dask_scheduler):
        """
        Instantiate a distributed dask scheduler
        Parameters
        ----------
          dask_scheduler: str
            URL of the dask scheduler
        """
        super().__init__()
        self.client = Client(dask_scheduler)
        self.client.forward_logging()
