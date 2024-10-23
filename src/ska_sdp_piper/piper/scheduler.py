# pragma: exclude file

from abc import ABC, abstractmethod
from typing import List

import dask

from .stage.stages import Stage


class PiperScheduler(ABC):
    """
    Scheduler interface required by piper to schedule the
    execution of configurable stages
    """

    @abstractmethod
    def schedule(self, stages: List[Stage]) -> None:
        """
        Contract for scheduling tasks

        Parameters
        ----------
            stages: list[stage.Stage]
               List of stages to be scheduled

        Returns
        -------
            None
        """
        pass

    @property
    @abstractmethod
    def tasks(self) -> List[dask.delayed]:
        """
        Property returning the list of scheduled tasks to be executed

        Returns
        -------
            list[dask.delayed.Delayed]
        """
        pass
