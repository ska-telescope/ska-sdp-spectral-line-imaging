# pragma: exclude file

from abc import ABC, abstractmethod, abstractproperty


class PiperScheduler(ABC):
    """
    Scheduler interface required by piper to schedule the
    execution of configurable stages
    """

    @abstractmethod
    def schedule(self, stages):
        """
        Contract for scheduling tasks

        Parameters
        ----------
            stages: list[Stages]
               List of stages to be scheduled

        Returns
        -------
            None
        """
        pass

    @abstractproperty
    def tasks(self):
        """
        Property returning the list of scheduled tasks to be executed

        Returns
        -------
            list[dask.Delayed]
        """
        pass
