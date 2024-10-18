from functools import reduce

from ska_sdp_piper.piper.scheduler import PiperScheduler

from .upstream_output import UpstreamOutput


class DefaultScheduler(PiperScheduler):
    """
    Schedules and executes dask wrapped functions on the local machine

    Attributes
    ----------
    _tasks: list(Delayed)
        Dask delayed outputs from the scheduled tasks
    """

    def __init__(self):
        self._stage_outputs = UpstreamOutput()

    def schedule(self, stages):
        """
        Schedules the stages as dask delayed objects

        Parameters
        ----------
          stages: list(stages.Stage)
            List of stages to schedule
        """
        self._stage_outputs = reduce(
            lambda output, stage: stage(output), stages, self._stage_outputs
        )

    def append(self, task):
        """
        Appends a dask task to the task list

        Parameters
        ----------
          task: Delayed
            Dask delayed object
        """
        self._stage_outputs.add_compute_tasks(task)

    def extend(self, tasks):
        """
        Extends the task list with a list of dask tasks

        Parameters
        ----------
          task: list(Delayed)
            Dask delayed objects
        """

        self._stage_outputs.add_compute_tasks(*tasks)

    @property
    def tasks(self):
        """
        Returns all the delayed task

        Returns
        -------
            list(Delayed)
        """
        return self._stage_outputs.compute_tasks
