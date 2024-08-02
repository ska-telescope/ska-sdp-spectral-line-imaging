from ..exceptions import NoStageToExecuteException, StageNotFoundException


class Stages:
    """
    Pipeline stages
    Attributes
    ----------
        __stages: list(Stage)
            List of pipeline stages
    """

    def __init__(self, stages=None):
        """
        Instantiate Stages object
        Parameters
        ----------
            stages: list(Stage)
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

    def validate(self, stage_names):
        """
        Validates the selected stage names with the pipeline definition stages.

        Parameters
        ----------
            stage_names: list(str)
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

    def update_pipeline_parameters(self, stage_names, stage_configs, **kwargs):
        """
        Update pipeline parameters for the stages
        Parameters
        ----------
            stage_name: list(str)
                List of stages for which pipeline parametere need to be updated
            stage_configs: dict
                Function returning the stage configuration
            **kwargs:
                Additional keyword arguments
        """

        for stage in self.__stages:
            if stage.name in stage_names:
                stage.update_pipeline_parameters(
                    stage_configs.get(stage.name, dict()), **kwargs
                )

    def get_stages(self, stage_names):
        """
        Returns the executable stages for the pipeline
        Parameters
        ----------
            stage_names: list(str)
                Stage names of stages to be returned
        Returns
        -------
            list(Stage)
        """
        return [stage for stage in self.__stages if stage.name in stage_names]
