import yaml

from ..io_utils import write_yml


class ConfigManager:
    """
    ConfigManager class responsible for handling all kinds of
      pipeline configurations including default config, CLI arguments
      and yaml config. CLI arguments being the highest priority.

    Attributes
    ----------
        pipeline: dict
            Pipeline stages state configuration.
            Dictionary containing stage name as key
            and boolean as value for enabled/disabled stage.
        parameters: dict
            Pipeline stages parameteres configuration.
            Dictionary containing states as key and a stages
            parameters dictionary as value.
    """

    def __init__(self, pipeline, parameters):
        """
        Initialise the config manager object.

        Parameters
        ----------
          pipeline: dict
              Pipeline stages state configuration.
          parameters: dict
              Pipeline stages parameteres configuration.
        """
        self.pipeline = pipeline
        self.parameters = parameters

    def update_config(self, config_path=None, pipeline=None):
        """
        Updates the configurations of the pipeline given
          from CLI and yaml file.

        Parameters
        ----------
        config_path: str
            Path to the config yaml file.
        pipeline: dict
            Pipeline stages state configuration.
             Dictionary containing stage name as key and boolean
             as value for enabled/disabled stage.
        """
        pipeline_from_config = None
        parameters = None

        if config_path:
            with open(config_path, "r") as config_file:
                config_dict = yaml.safe_load(config_file)
                pipeline_from_config = config_dict.get("pipeline", dict())
                parameters = config_dict.get("parameters", dict())

        if parameters:
            self.parameters = {
                key: {**value, **parameters.get(key, dict())}
                for key, value in self.parameters.items()
            }

        self.__update_pipeline(pipeline_from_config)
        self.__update_pipeline(pipeline)

    def __update_pipeline(self, pipeline=None):
        if pipeline:
            self.pipeline = {
                key: pipeline.get(key, False) for key in self.pipeline
            }

    def stage_config(self, stage_name):
        """
        Returns stage parameter config given the stage name.

        Parameters
        ----------
        stage_name: str
            Stage name.

        Returns
        -------
        dict
            Stage parameters dictionary.
        """
        return self.parameters.get(stage_name, dict())

    def write_yml(self, path):
        """
        Writes config to provided path in yaml format.

        Parameters
        ----------
        path: str
            Location of config file to write to.
        """
        write_yml(path, self.config)

    @property
    def config(self):
        """
        Get all the configuration.

        Returns
        -------
        configuration: dict(parameters, pipelines)
            Whole pipeline configuration as a dictionary containing
            pipelines and parameters as top level config entries.
        """
        return {"parameters": self.parameters, "pipeline": self.pipeline}

    @property
    def stages_to_run(self):
        """
        Returns the stages to run.

        Returns
        -------
        [str]:
            List of stages names to run.
        """
        return [stage for stage in self.pipeline if self.pipeline.get(stage)]
