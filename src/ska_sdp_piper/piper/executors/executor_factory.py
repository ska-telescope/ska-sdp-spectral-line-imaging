from .default_executor import DefaultExecutor
from .distributed_executor import DistributedExecutor


class ExecutorFactory:
    """
    Select an appropriate scheduler based on conditions
    """

    @staticmethod
    def get_executor(output_dir, dask_scheduler=None, **kwargs):
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
           :py:class:`DefaultExecutor`
           :py:class:`DaskExecutor`
        """
        if dask_scheduler:
            return DistributedExecutor(dask_scheduler, output_dir, **kwargs)

        return DefaultExecutor()
