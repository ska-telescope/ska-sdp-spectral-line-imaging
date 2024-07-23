import inspect

from .configuration import Configuration
from .exceptions import PipelineMetadataMissingException


class ConfigurableStage:
    """
    Parameterized decorator to define a configurable stage

    Attributes
    ----------
       name: str
            The name of the stage.
       _stage_configuration: Configuration
            Configuration parameters for the stage
    """

    def __init__(self, name, configuration=None):
        """
        Initialise the ConfigurableStage class

        Parameters
        ----------
            name: str
                Name of the stage
            configuration: Configuration
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

        stage = Stage(self.name, stage_definition, self._stage_configurations)

        return stage


class Stage:
    """
    Decorator for the stage definition function

    Attributes
    ----------
        name: str
            Name of the stage
        stage_definition: function
            Function being decorated
        params: list(str)
            Argument list of stage definition
        __config: Configuration
            Configuration for the stage
        __pipeline_parameters: dict
            Stores runtime parameters of the pipeline
    """

    def __init__(self, name, stage_definition, configuration):
        """
        Initialize the Stage object

        Parameters
        ----------
            name: str
                Name of the stage
            stage_definition: function
                Function to be decorated
            configuration: Configuration
                Configuration for the stage
        """
        self.name = name
        self.stage_definition = stage_definition
        self.stage_definition.name = name
        self.params = inspect.getfullargspec(stage_definition).args

        self.__config = configuration
        self.__pipeline_parameters = None

        self.__config.valididate_arguments_for(self)

    @property
    def config(self):
        """
        Stage configuration dictionary.

        Returns
        -------
           dict
        """
        return {self.name: self.__config.items}

    def update_pipeline_parameters(self, config, **kwargs):
        """
        Updates stage's pipeline parameters

        Parameters
        ----------
            config: dict
                Stage configurations from the pipeline
            **kwargs:
                Additional keyword arguments from the pipeline
        """
        self.__pipeline_parameters = dict(config=config, kwargs=kwargs)

    def get_stage_arguments(self):
        """
        Returns the runtime argument for the stage definition

        Returns
        -------
           dict

        Raises
        ------
            PipelineMetadataMissingException:
                If stage's pipeline parameters are not initialized
        """
        if self.__pipeline_parameters is None:
            raise PipelineMetadataMissingException(
                f"Pipeline parameters not initialised for {self.name}"
            )
        non_positional_arguments = self.params[1:]
        additional_params = (
            set(non_positional_arguments)
            - self.__pipeline_parameters["config"].keys()
        )
        return {
            **self.__pipeline_parameters["config"],
            **{
                keyword: self.__pipeline_parameters["kwargs"][keyword]
                for keyword in additional_params
            },
        }
