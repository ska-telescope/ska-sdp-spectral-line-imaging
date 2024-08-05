from functools import wraps

from ..configurations import Configuration
from .stages import Stage


class ConfigurableStage:
    """
    Parameterized decorator to define a configurable stage

    Attributes
    ----------
       name: str
            The name of the stage.
       _stage_configuration: configurations.Configuration
            Configuration parameters for the stage
    """

    def __init__(self, name, configuration=None):
        """
        Initialise the ConfigurableStage class

        Parameters
        ----------
            name: str
                Name of the stage
            configuration: configurations.Configuration
                Configuration for the stage
        """

        self._stage_configurations = (
            Configuration() if configuration is None else configuration
        )
        self.name = name

    def __call__(self, stage_definition):
        """
        Provides a callable object for the next level of function wrapping

        Parameters
        ----------
            stage_definition: function
                The function being decorated

        Returns
        -------
            Wrapped stage_definition
        """

        stage = wraps(stage_definition)(
            Stage(self.name, stage_definition, self._stage_configurations)
        )

        return stage
