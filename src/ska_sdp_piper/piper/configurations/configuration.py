import logging

from ..exceptions import (
    ArgumentMismatchException,
    PipelineMetadataMissingException,
)
from .config_groups import ConfigGroups

logger = logging.getLogger()


class Configuration(ConfigGroups):
    """
    Class containing all the configurations for a stage

    Attributes
    ----------
        _config_params: dict(str -> ConfigParam)
            Configuration parameters for the stage
    """

    @property
    def items(self):
        """
        Configuration parameters

        Returns
        -------
           Dictionary of configuration parameters with default values
        """
        return {key: param.value for key, param in self._config_params.items()}

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
        configuration_keys = set(self._config_params.keys())

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
