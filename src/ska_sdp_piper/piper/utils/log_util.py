import logging

import dask
import dask.distributed
from distributed import WorkerPlugin
from ska_ser_logging import configure_logging


class LogUtil:
    @classmethod
    def configure(cls, log_file=None, verbose=False):
        """
        Configure the log using standardised config

        Parameters
        ----------
            log_file: str
                Path to log file
            verbose: bool
                Set log verbosity to DEBUG

        """

        level = logging.INFO
        if verbose:
            level = logging.DEBUG

        configure_logging(
            level=level,
            overrides=cls.__additional_log_config(log_file),
        )

    @classmethod
    def __additional_log_config(cls, log_file):
        """
        Get updated log configuration

        Parameters
        ----------
            log_file: str
                Path to log file

        Returns
        -------
            dictionary config
        """
        if log_file is None:
            return

        return {
            "handlers": {
                "file": {
                    "()": logging.FileHandler,
                    "formatter": "default",
                    "filename": log_file,
                }
            },
            "root": {
                "handlers": ["console", "file"],
            },
        }


class LogPlugin(WorkerPlugin):
    def __init__(self, log_file=None, verbose=False):
        self.log_file = log_file
        self.verbose = verbose

    def setup(self, worker):
        LogUtil.configure(self.log_file, self.verbose)


@dask.delayed
def delayed_log(logger, formated_log_msg, _level_="info", **kwargs):
    outputs = kwargs.copy()
    for key, value in kwargs.items():
        if isinstance(value, list):
            outputs[key] = value[1](value[0])
    getattr(logger, _level_)(formated_log_msg.format(**outputs))
