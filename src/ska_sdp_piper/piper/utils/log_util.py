import logging

import dask
import dask.distributed
from distributed.diagnostics.plugin import WorkerPlugin
from ska_ser_logging import configure_logging

from .io_utils import timestamp


class LogUtil:
    @classmethod
    def configure(cls, name, output_dir=None, verbose=False):
        """
        Configure the log using standardised config

        Parameters
        ----------
            name: str
                Pipeline name
            verbose: bool
                Set log verbosity to DEBUG

        """

        level = logging.INFO
        if verbose:
            level = logging.DEBUG

        configure_logging(
            level=level,
            overrides=cls.__additional_log_config(name, output_dir),
        )

    @classmethod
    def __additional_log_config(cls, pipeline_name, output_dir=None):
        """
        Get updated log configuration

        Parameters
        ----------
            pipeline_name: str
                Log file name
            output_dir: str
                Output directory

        Returns
        -------
            dictionary config
        """
        if output_dir is None:
            return

        log_file = f"{output_dir}/{pipeline_name}_" f"{timestamp()}.log"
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
    def __init__(self, name, output_dir, verbose):
        self.name = name
        self.output_dir = output_dir
        self.verbose = verbose

    def setup(self, worker):
        LogUtil.configure(self.name, self.output_dir, self.verbose)


@dask.delayed
def delayed_log(logger, formated_log_msg, _level_="info", **kwargs):
    outputs = kwargs.copy()
    for key, value in kwargs.items():
        if isinstance(value, list):
            outputs[key] = value[1](value[0])
    getattr(logger, _level_)(formated_log_msg.format(**outputs))
