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
            Stage object wrapping the stage_definition
        """

        self._stage_configurations.valididate_arguments_for(stage_definition)

        return Stage(stage_definition, self._stage_configurations)


class Stage:
    """
    Decorator for the stage definition function
    Attributes:
       _stage_definition (func): The function being decorated
       _stage_configuration (Configuration): Configuration for the stage

    """

    def __init__(self, stage_definition, configuration=None):
        """
        Initialize the _Stage object
        Parameters:
            stage_definition (func): Function to be decorated
            configuration (Configuration): The stage configuration
        """
        self._stage_definition = stage_definition
        self._stage_configurations = configuration

    def __call__(self, pipeline_data, **kwargs):
        """
        Provide callable object for the wrapped function
        Parameters:
           vis (DataArray): MSv4 data array
           **kwargs (dict): Other Key-Value arguments
        Returns:
           DataArray: Processed visibility data
        """
        updated_kwargs = kwargs
        updated_kwargs = self._stage_configurations.extend(**kwargs)

        return self._stage_definition(pipeline_data, **updated_kwargs)
