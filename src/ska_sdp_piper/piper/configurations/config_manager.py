from ..utils.io_utils import read_yml, write_yml


class ConfigManager:
    """
    ConfigManager class responsible for handling all kinds of
    pipeline configurations including default config, CLI arguments
    and yaml config. CLI arguments being the highest priority.

    Attributes
    ----------
    pipeline: dict
        Pipeline stages state configuration.
        Dictionary containing stage name as key and
        boolean as value for enabled/disabled stage.
    parameters: dict
        Pipeline stages parameteres configuration.
        Dictionary containing states as key and
        a stages parameters dictionary as value.
    """

    def __init__(self, pipeline, parameters, global_parameters):
        """
        Initialise the config manager object.

        Parameters
        ----------
            pipeline: dict
                Pipeline stages state configuration.
            parameters: dict
                Pipeline stages parameters configuration.
            global_parameters: dict
                Pipeline global parameters configuration.
        """
        self.pipeline = pipeline
        self.parameters = parameters
        self.global_parameters = global_parameters

    def update_config(self, config_path):
        """
        Updates the configurations of the pipeline given
            from CLI and yaml file.

        Parameters
        ----------
            config_path: str
                Path to the config yaml file.
        """

        config_dict = read_yml(config_path)
        pipeline_from_config = config_dict.get("pipeline", dict())
        parameters = config_dict.get("parameters", dict())
        global_params = config_dict.get("global_parameters", dict())

        self.parameters = {
            key: {**value, **parameters.get(key, dict())}
            for key, value in self.parameters.items()
        }

        self.global_parameters = {
            **self.global_parameters,
            **global_params,
        }

        if pipeline_from_config:
            self.update_pipeline(pipeline_from_config)

    def update_pipeline(self, pipeline):
        """
        Update pipeline property of the the config

        Parameters
        ----------
            pipeline: dict
                Pipeline stages state configuration.
                Dictionary containing stage name as key and boolean
                as value for enabled/disabled stage.
        """

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

    def set(self, path, value):
        """
        Updates a given key in the config

        Parameters
        ----------
            path: str
                yaml/json path to the key
            value: any
                Value to be update with
        """
        path_elements = path.split(".")
        ref = self.config
        for elem in path_elements[:-1]:
            ref = ref.get(elem, {})

        # If path is valid update the key
        if not ref == {}:
            ref[path_elements[-1]] = value

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
        return {
            "parameters": self.parameters,
            "pipeline": self.pipeline,
            "global_parameters": self.global_parameters,
        }

    @property
    def stages_to_run(self):
        """
        Returns the stages to run.

        Returns
        -------
        list(str)
            List of stages names to run.
        """
        return [stage for stage in self.pipeline if self.pipeline.get(stage)]
