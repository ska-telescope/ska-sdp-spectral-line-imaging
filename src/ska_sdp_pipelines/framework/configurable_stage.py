from .configuration import Configuration


class ConfigurableStage:
    """
    Parameterized ecorator to define a configurable stage

    Attributes:
       _name (str): The name of the stage.
       _stage_configuration (Configuration): Configuration parameters for
                                            the stage
    """

    def __init__(self, name, configuration=None):
        """
        Initialise the ConfigurableStage class
        Parameters:
            name (str): Name of the stage
            configuration (Configuration) : Configuration for the stage
        """

        self._stage_configurations = (
            Configuration() if configuration is None else configuration
        )
        self._name = name

    def __call__(self, stage_definition):
        """
        Provides a callable object for the next level of function wrapping
        Parameters:
            stage_definition (function): The function being decorated
        Returns:
            Wrapped stage_definition
        """

        self._stage_configurations.valididate_arguments_for(stage_definition)

        stage_definition.name = self._name
        stage_definition.stage_config = self._stage_configurations
        stage_definition.config = {
            self._name: self._stage_configurations.items
        }
        return stage_definition
