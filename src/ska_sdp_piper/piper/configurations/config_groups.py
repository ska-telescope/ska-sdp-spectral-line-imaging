import logging

logger = logging.getLogger()


class ConfigGroups:
    def __init__(self, **kwargs):
        """
        Initialise a Configuration object.

        Parameters
        ----------
           kwargs:
               ConfigParam objects
        """
        self._config_params = kwargs

    def update_config_params(self, **kwargs):
        """
        Update the value of the configuration parameter

        Parameters
        ----------
            **kwargs: Key word arguments

        Raises
        ------
            TypeError:
               If the update value doesn't match the type of the config param

        """

        for key, value in kwargs.items():
            if key in self._config_params:
                config_param = self._config_params[key]
                config_param.value = value
            else:
                logger.warning(
                    f'Property "{key}" is invalid. Ignoring '
                    "and continuing the pipeline."
                )
