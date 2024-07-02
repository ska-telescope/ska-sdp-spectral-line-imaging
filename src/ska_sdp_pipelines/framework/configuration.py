import inspect

from .exceptions import ArgumentMismatchException, VisibilityMissingException


class ConfigParam:
    """
    Configuration Parameters

    Attributes:
       _type(str): Type of the the configurable parameter.
       default (_type): The default value for the configurable parameter.
       description (str): Description of the configurable parameter.
    """

    def __init__(self, _type, default, description=None):
        """
        Initialise a ConfigParam object

        Parameters:
            _type(str): Type of the the configurable parameter.
            default (_type): The default value for the configurable parameter.
            description (str): Description of the configurable parameter.
        """
        self._type = _type
        self.default = default
        self.description = description


class Configuration:
    """
    Class containing all the configurations for a stage
    Attributes:
        __config_params (dict(str -> ConfigParam)): Configuration parameters
                                                    for the stage
    """

    def __init__(self, **kwargs):
        """
        Initialise a Configuration object.
        Parameters:
           kwargs: ConfigParam objects
        """
        self.__config_params = kwargs

    def valididate_arguments_for(self, stage_definition):
        """
        Validates if the arguments provided for the stage contains all the
        mandatory and configurable parameters

        Parameters:
           stage_definition (func): The function defining the stage
        Raises:
            VisibilityMissingException: If mandatory argument 'vis' is missing
                                        in the parameter list of the stage
                                        definition
            ArgumentMismatchException: If the config parameters are not present
                                       in the parameter list of the stage
                                       definition
        """

        stage_arguments = inspect.getargspec(stage_definition).args
        configuration_keys = set(self.__config_params.keys())

        if (
            len(stage_arguments) == 0
            or stage_arguments[0] in configuration_keys
        ):
            raise VisibilityMissingException(
                "Mandatory argument vis missing in argument list"
            )

        configuration_variables = set(stage_arguments[1:])
        if configuration_variables != configuration_keys:
            raise ArgumentMismatchException("Invalid argument list")

    def extend(self, **kwargs):
        """
        Updates the function parameters with the configuration parameter values
        """
        return {
            **{
                key: value.default
                for key, value in self.__config_params.items()
            },
            **kwargs,
        }


__all__ = ["ConfigParam", "Configuration"]
