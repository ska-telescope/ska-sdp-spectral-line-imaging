# pragma: exclude file

from abc import ABC, abstractmethod, abstractproperty


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

    @abstractproperty
    def tasks(self):
        pass
