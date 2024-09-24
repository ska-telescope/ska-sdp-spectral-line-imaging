import dask


class DefaultScheduler:
    """
    Schedules and executes dask wrapped functions on the local machine

    Attributes
    ----------
      tasks: [dask.delayed]
        Dask delayed outputs from the scheduled tasks
    """

    def __init__(self):
        self.tasks = []

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

            self.tasks.append(output)

    def append(self, task):
        """
        Appends a dask task to the task list

        Parameters
        ----------
          task: dask.delayed
            Dask delayed object
        """

        self.tasks.append(task)

    def extend(self, tasks):
        """
        Extends the task list with a list of dask tasks

        Parameters
        ----------
          task: list[dask.delayed]
            Dask delayed objects
        """

        self.tasks.extend(tasks)
