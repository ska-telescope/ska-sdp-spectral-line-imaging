from functools import reduce

import yaml

from ..constants import ConfigRoot
from ..exceptions import StageNotFoundException
from ..utils.io_utils import read_yml, write_yml


class RuntimeConfig:
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

    def set(self, config_path, value):
        """
        Updates a given key in the config

        Parameters
        ----------
            path: str
                yaml/json path to the key
            value: any
                Value to be update with
        """
        root_path, *path = config_path.split(".")
        if root_path == ConfigRoot.GLOBAL_PARAMETERS:
            self.global_parameters = value

        elif root_path == ConfigRoot.PARAMETERS:
            if path[0] not in self.parameters:
                raise StageNotFoundException(path[0])
            config_param = reduce(
                lambda acc, val: acc.get(val), path[:-1], self.parameters
            )
            if config_param is None:
                raise ValueError(
                    f"Path {'.'.join(path)} not found in configuration yaml"
                )

            config_param[path[-1]] = value

        elif root_path == ConfigRoot.PIPELINE:
            if type(value) is not bool:
                raise ValueError(
                    f"Stage flags need to be boolean, {type(value)} provided"
                )
            if path[0] not in self.pipeline:
                raise StageNotFoundException(path[0])
            if len(path) != 1:
                raise ValueError("Illegal stage name parameter provided")

            self.pipeline[path[0]] = value

        else:
            raise ValueError(f"Invalid configuration path {config_path}")

    def update_from_yaml(self, yaml_path):
        """
        Update the runtime configuration from a yaml file

        Parameters
        ----------
            yaml_path: str
                Path to the yaml configuration file

        Returns
        -------
            RuntimeConfig
        """

        if not yaml_path:
            return self

        config_dict = read_yml(yaml_path)
        self.pipeline = {
            **self.pipeline,
            **config_dict.get(ConfigRoot.PIPELINE, dict()),
        }
        self.parameters = {
            **self.parameters,
            **config_dict.get(ConfigRoot.PARAMETERS, dict()),
        }
        self.global_parameters = {
            **self.global_parameters,
            **config_dict.get(ConfigRoot.GLOBAL_PARAMETERS, dict()),
        }

        return self

    def update_from_cli_overrides(self, cli_overrides):
        """
        Update the runtime configuration from CLI overrides option

        Parameters
        ----------
            cli_overrides: list[list[str, any]]
                CLI override options

        Returns
        -------
            RuntimeConfig
        """

        if not cli_overrides:
            return self
        params_to_update = yaml.safe_load(
            "\n".join(f"{a} : {b}" for a, b in cli_overrides)
        )

        for (path, value) in params_to_update.items():
            self.set(path, value)

        return self

    def update_from_cli_stages(self, cli_stages):
        """
        Update the pipeline stage states

        Parameters
        ----------
            cli_stages: list[str]
                Stage names to be enabled

        Returns
        -------
            RuntimeConfig
        """

        if not cli_stages:
            return self
        self.pipeline = {stage: stage in cli_stages for stage in self.pipeline}

        return self

    def write_yml(self, path):
        """
        Writes config to provided path in yaml format.

        Parameters
        ----------
            path: str
                Location of config file to write to.
        """

        config = {
            ConfigRoot.PARAMETERS: self.parameters,
            ConfigRoot.PIPELINE: self.pipeline,
            ConfigRoot.GLOBAL_PARAMETERS: self.global_parameters,
        }
        write_yml(path, config)

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
