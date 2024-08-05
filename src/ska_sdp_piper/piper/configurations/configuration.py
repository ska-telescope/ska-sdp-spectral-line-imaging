from ..exceptions import (
    ArgumentMismatchException,
    PipelineMetadataMissingException,
)


class ConfigParam:
    """
    Configuration Parameters

    Attributes
    ----------
       _type: str
           Type of the configurable parameter.
       default: _type
           The default value for the configurable parameter.
       description: str
           Description of the configurable parameter.
    """

    def __init__(self, _type, default, description=None):
        """
        Initialise a ConfigParam object

        Parameters
        ----------
            _type: str
                Type of the configurable parameter.
            default: _type
                The default value for the configurable parameter.
            description: str
                Description of the configurable parameter.
        """
        self._type = _type
        self.default = default
        self.description = description


class Configuration:
    """
    Class containing all the configurations for a stage

    Attributes
    ----------
        __config_params: dict(str -> ConfigParam)
            Configuration parameters for the stage
    """

    def __init__(self, **kwargs):
        """
        Initialise a Configuration object.

        Parameters
        ----------
           kwargs:
               ConfigParam objects
        """
        self.__config_params = kwargs

    @property
    def items(self):
        """
        Configuration parameters

        Returns
        -------
           Dictionary of configuration parameters with default values
        """
        return {
            key: value.default for key, value in self.__config_params.items()
        }

    def valididate_arguments_for(self, stage):
        """
        Validates if the arguments provided for the stage contains all the
        mandatory and configurable parameters

        Parameters
        ----------
           stage_definition: func
               The function defining the stage

        Raises
        ------
            PipelineMetadataMissingException:
                If mandatory argument 'vis' is missing
                in the parameter list of the stage definition
            ArgumentMismatchException:
                If the config parameters are not present
                in the parameter list of the stage definition
        """

        stage_arguments = stage.params
        configuration_keys = set(self.__config_params.keys())

        if (
            len(stage_arguments) == 0
            or stage_arguments[0] in configuration_keys
        ):
            raise PipelineMetadataMissingException(
                "Mandatory first argument pipeline metadata missing"
            )

        configuration_variables = set(stage_arguments)
        if not configuration_variables.issuperset(configuration_keys):
            raise ArgumentMismatchException("Invalid argument list")

    def extend(self, **kwargs):
        """
        Updates the function parameters with the configuration parameter values
        """
        return {
            **self.items,
            **kwargs,
        }
