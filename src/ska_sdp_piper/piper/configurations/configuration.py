from ..exceptions import (
    ArgumentMismatchException,
    PipelineMetadataMissingException,
)


class ConfigParam:
    """
    Configuration Parameters

    Attributes
    ----------
       _type: type
           Type of the configurable parameter.
       value: _type
           The value for the configurable parameter.
       description: str
           Description of the configurable parameter.
    """

    def __init__(self, _type, default, description=None):
        """
        Initialise a ConfigParam object

        Parameters
        ----------
            _type: type
                Type of the configurable parameter.
            default: _type
                The default value for the configurable parameter.
            description: str
                Description of the configurable parameter.
        """
        self._type = _type
        self.value = default
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
            key: param.value for key, param in self.__config_params.items()
        }

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
            config_param = self.__config_params[key]
            if value is not None and type(value) is not config_param._type:
                raise TypeError(
                    "Parameter type does not match:"
                    f" {config_param._type} expected, {type(value)} provided"
                )

            config_param.value = value

    def valididate_arguments_for(self, stage):
        """
        Validates if the arguments provided for the stage contains all the
        mandatory and configurable parameters

        Parameters
        ----------
           stage: stages.Stage
               A stage of the pipeline

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
