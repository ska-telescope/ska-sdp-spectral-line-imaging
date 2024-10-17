import dask


class DefaultExecutor:
    """
    Schedules and executes dask wrapped functions on the local machine

    Attributes
    ----------
        Dask delayed outputs from the scheduled tasks
    """

    def execute(self, tasks):
        """
        Executes the scheduled stages.
        Since the default scheduler is dask based, the execute calls the
        compute on the scheduled dask graph

        Parameters
        ----------
          tasks: list[dask.delayed]
            Dask delayed outputs from the scheduled tasks

        """
        return dask.compute(*tasks, optimize_graph=True)
