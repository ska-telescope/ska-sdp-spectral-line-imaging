# pragma: exclude file

from abc import ABC, abstractmethod


class PiperScheduler(ABC):
    @abstractmethod
    def schedule(self, stages, verbose=False):
        pass

    @abstractmethod
    def append(self, task):
        pass

    @abstractmethod
    def extend(self, tasks):
        pass

    @property
    @abstractmethod
    def tasks(self):
        pass
