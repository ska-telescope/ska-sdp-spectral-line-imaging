import dask

from ska_sdp_piper.piper.scheduler import PiperScheduler


class DefaultScheduler(PiperScheduler):
    """
    Schedules and executes dask wrapped functions on the local machine

    Attributes
    ----------
    _tasks: list(Delayed)
        Dask delayed outputs from the scheduled tasks
    """

    def __init__(self):
        self._tasks = []

    def schedule(self, stages, verbose=False):
        """
        Schedules the stages as dask delayed objects

        Parameters
        ----------
          stages: list(stages.Stage)
            List of stages to schedule
          verbose: bool
            Log debug statements
        """
        output = None
        for stage in stages:
            output = dask.delayed(stage)(output, verbose)

            self._tasks.append(output)

    def append(self, task):
        """
        Appends a dask task to the task list

        Parameters
        ----------
          task: Delayed
            Dask delayed object
        """

        self._tasks.append(task)

    def extend(self, tasks):
        """
        Extends the task list with a list of dask tasks

        Parameters
        ----------
          task: list(Delayed)
            Dask delayed objects
        """

        self._tasks.extend(tasks)

    @property
    def tasks(self):
        """
        Returns all the delayed task

        Returns
        -------
            list(Delayed)
        """
        return self._tasks