from .config_param import ConfigParam


class NestedConfigParam(ConfigParam):
    """
    Nested Configuration Parameters

    Attributes
    ----------
        _type: type
            Type of the configurable parameter.
        description: str
            Description of the configurable parameter.
    """

    def __init__(self, description, **config_params):
        """
        Initialise a NestedConfigParam object

        Parameters
        ----------
            description: str
                Description of the configurable parameter.
           config_params:
               ConfigParam objects

        """

        self.__config_params = config_params
        self.description = description
        self._type = NestedConfigParam

    def __getitem__(self, param):
        return self.__config_params[param].value

    def get(self, param, default=None):
        """
        Get config params or defaults
        """
        config_param = self.__config_params.get(param)
        return default if config_param is None else config_param.value

    @property
    def value(self):
        """
        Value property
        """
        return {
            key: config.value for key, config in self.__config_params.items()
        }

    @value.setter
    def value(self, new_value):
        """
        Value setter

        Parameters
        ----------
            new_value: dict
                Update values for parameters within the nested configuration.

        Raises
        ------
            TypeEror: If new_value is None or not a dict
            KeyError: If new_value contains items not already present in the
                nested configuration
        """

        if not new_value or not isinstance(new_value, dict):
            raise TypeError(
                "Trying to set primitive value to a nested config parameter"
            )

        for key, value in new_value.items():
            config_param = self.__config_params[key]
            config_param.value = value
