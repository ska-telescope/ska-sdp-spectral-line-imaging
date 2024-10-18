from functools import reduce

from ..exceptions import StageNotFoundException
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

    def _set_global(self, value={}, **kwargs):
        self.global_parameters = {**self.global_parameters, **value}

    def _set_pipeline(self, path, value, **kwargs):
        if type(value) is not bool:
            raise ValueError(
                f"Stage flags need to be boolean, {type(value)} provided"
            )
        if path[0] not in self.pipeline:
            raise StageNotFoundException(path[0])
        if len(path) != 1:
            raise ValueError("Illegal stage name parameter provided")

        self.pipeline[path[0]] = value

    def _set_parameters(self, path, value, **kwargs):
        config_param = reduce(
            lambda acc, val: acc.get(val), path[:-1], self.parameters
        )
        if config_param is None:
            raise ValueError(
                f"Path {'.'.join(path)} not found in configuration yaml"
            )

        ref = config_param[path[-1]]
        expected_type = type(ref)
        actual_type = type(value)
        # TODO: This handling of None type bypasses type check,
        # and should be removed in the future
        if expected_type is not actual_type and ref is not None:
            raise ValueError(
                f"Type of value for {path} is {actual_type} should"
                f" be {expected_type}"
            )
        config_param[path[-1]] = value

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
        config_setters = {
            "global_parameters": self._set_global,
            "parameters": self._set_parameters,
            "pipeline": self._set_pipeline,
        }

        if path_elements[0] not in config_setters:
            raise ValueError(f"Invalid configuration path {path}")

        config_setters[path_elements[0]](path=path_elements[1:], value=value)

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
