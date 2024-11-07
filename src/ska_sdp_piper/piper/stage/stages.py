import inspect

from ska_sdp_piper.piper.exceptions import (
    NoStageToExecuteException,
    PipelineMetadataMissingException,
    StageNotFoundException,
)


class Stage:
    """
    Decorator for the stage definition function

    Attributes
    ----------
        name: str
            Name of the stage
        stage_definition: function
            Function being decorated
        params: list[str]
            Argument list of stage definition
        __config: configurations.Configuration
            Configuration for the stage
        __additional_parameters: dict
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
        self.params = inspect.getfullargspec(stage_definition).args

        self.__config = configuration
        self.__additional_parameters = None

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

    def update_parameters(self, **kwargs):
        """
        Update the configuration parameter

        Parameters
        ----------
            **kwargs: Key word arguments
        """
        self.__config.update_config_params(**kwargs)

    def add_additional_parameters(self, **kwargs):
        """
        Updates stage's pipeline parameters

        Parameters
        ----------
            config: dict
                Stage configurations from the pipeline
            **kwargs:
                Additional keyword arguments from the pipeline
        """
        self.__additional_parameters = kwargs

    def __call__(self, upstream_output):
        """
        Execute stage definition with upstream output
        and prepared paramenters

        Parameters
        ----------
            upstream_output: Any
                Output from the upstream stage
        """

        if self.__additional_parameters is None:
            raise PipelineMetadataMissingException(
                f"Pipeline parameters not initialised for {self.name}"
            )

        non_positional_arguments = self.params[1:]
        config_args = self.__config.items
        additional_params = set(non_positional_arguments) - config_args.keys()
        stage_args = {
            **config_args,
            **{
                keyword: self.__additional_parameters[keyword]
                for keyword in additional_params
            },
        }

        output = self.stage_definition(upstream_output, **stage_args)
        return output


class Stages:
    """
    Pipeline stages

    Attributes
    ----------
        __stages: list[Stage]
            List of pipeline stages
    """

    def __init__(self, stages=None):
        """
        Instantiate Stages object

        Parameters
        ----------
            stages: list[Stage]
                List of pipeline stages
        """

        self.__stages = [] if stages is None else stages

    def __iter__(self):
        """
        Iterator for stages

        Returns
        -------
            Iterable(stage)
        """

        return self.__stages.__iter__()

    def update_stage_parameters(self, stage_parameters):
        """
        Update the configurable parameters of stages

        Parameters
        ----------
            stage_parameters: dict(str -> dict)
                Stage configuration parameters

        """
        for stage in self.__stages:
            if stage.name in stage_parameters:
                stage.update_parameters(**stage_parameters[stage.name])

    def validate(self, stage_names):
        """
        Validates the selected stage names with the pipeline definition stages.

        Parameters
        ----------
            stage_names: list[str]
                List Stage names

        Raises
        ------
            StageNotFoundException
                If any non-existing stage found.
            NoStageToExecuteException
                If no stages are selected for execution
        """

        if not stage_names:
            raise NoStageToExecuteException("Selected stages empty")

        _stage_names = [stage.name for stage in self.__stages]

        non_existing_stages = [
            stage for stage in stage_names if stage not in _stage_names
        ]

        if non_existing_stages:
            raise StageNotFoundException(
                f"Stages not found: {non_existing_stages}"
            )

    def add_additional_parameters(self, **kwargs):
        """
        Update pipeline parameters for the stages

        Parameters
        ----------
            **kwargs:
                Additional keyword arguments
        """

        for stage in self.__stages:
            stage.add_additional_parameters(**kwargs)

    def get_stages(self, stage_names):
        """
        Returns the executable stages for the pipeline

        Parameters
        ----------
            stage_names: list[str]
                Stage names of stages to be returned
        Returns
        -------
            list[Stage]
        """
        return [stage for stage in self.__stages if stage.name in stage_names]
