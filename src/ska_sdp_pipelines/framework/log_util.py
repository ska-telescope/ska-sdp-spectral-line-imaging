import logging
from datetime import datetime

from ska_ser_logging import configure_logging


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

        timestamp = datetime.now()
        log_file = (
            f"{output_dir}/{pipeline_name}_"
            f"{timestamp.strftime('%Y-%m-%dT%H:%M:%S')}.log"
        )
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

    @classmethod
    def with_log(cls, verbose, stage, *args, **kwargs):
        """
        Execute stage with entry and exit log

        Parameters
        ---------
            verbose: bool
                Set verbosity level
            stage: functions
                Stage function
            pipeline_data: dict
                Pipeline data for the stage
            **kwargs
                Additional arguments

        Returns
        -------
            Stage output
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        if verbose:
            logger.setLevel(logging.DEBUG)
        logger.info(f"=============== START {stage.name} ============= ")
        output = stage(*args, **kwargs)
        logger.info(f"=============== FINISH {stage.name} ============ ")
        return output
