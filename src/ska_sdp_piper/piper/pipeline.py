import functools
import logging

import yaml

from .command import Command
from .configurations import Configuration
from .configurations.config_manager import ConfigManager
from .constants import CONFIG_CLI_ARGS, DEFAULT_CLI_ARGS
from .executors import ExecutorFactory
from .named_instance import NamedInstance
from .utils import LogUtil, create_output_dir, timestamp


class Pipeline(Command, metaclass=NamedInstance):
    """
    Pipeline class allows for defining a pipeline as an ordered list of
    stages, and takes care of executing those stages.

    Attributes
    ----------
      name: str
          Name of the pipeline
      _stages: list[stage.ConfigurableStage]
          Stage to be executed
    """

    def __init__(
        self,
        name,
        stages=None,
        scheduler=None,
        global_config=None,
        cli_args=None,
        **kwargs,
    ):
        """
        Initialise the pipeline object

        Parameters
        ----------
          name: str
              Name of the pipeline
          stages: piper.stages.Stages
              Stages to be executed
          global_config: Configuration
              Pipeline level configurations
          cli_args: list[CLIArgument]
              Runtime arguments for the pipeline
          **kwargs:
              Additional kwargs
        """
        super().__init__()
        self.name = name
        self._global_config = (
            Configuration() if global_config is None else global_config
        )

        self.logger = logging.getLogger(self.name)

        self._stages = stages

        self.config_manager = ConfigManager(
            pipeline=self._pipeline_config(),
            parameters=self._parameter(),
            global_parameters=self._global_config.items,
        )

        self.scheduler = scheduler

        self.sub_command(
            "run",
            DEFAULT_CLI_ARGS + ([] if cli_args is None else cli_args),
            help="Run the pipeline",
        )(self._run)

        self.sub_command(
            "install-config",
            CONFIG_CLI_ARGS,
            help="Installs the default config at --config-install-path",
        )(self._install_config)

    def _pipeline_config(self, selected_stages=None):
        """
        Returns the pipeline config as a dictionary of enabled/disabled stages.

        Parameters
        ----------
        selected_stages: [str]
            Enabled stages.

        Returns
        -------
        dict
            Dictionary of selected stages
        """

        if selected_stages:
            return {
                stage.name: stage.name in selected_stages
                for stage in self._stages
            }

        return {stage.name: True for stage in self._stages}

    def _parameter(self):
        """
        Returns pipeline stages config parameters dictionary.

        Returns
        -------
        dict
            Dictionary of stage parameters
        """

        return functools.reduce(
            lambda config, stage: {**config, **stage.config}, self._stages, {}
        )

    @property
    def config(self):
        """
        Get all the configuration

        Returns
        -------
        dict
            Pipeline configuration with top level keys
                - `parameters`
                - `pipelines`
        """
        return self.config_manager.config

    def _run(self, cli_args):
        """
        Run sub command

        Parameters
        ----------
            cli_args: argparse.Namespace
                CLI arguments
        """
        stages = [] if cli_args.stages is None else cli_args.stages[0]

        output_path = (
            "./output"
            if cli_args.output_path is None
            else cli_args.output_path
        )
        output_dir = create_output_dir(output_path, self.name)

        cli_output_file = f"{output_dir}/{self.name}_{timestamp()}.cli.yml"
        self._cli_command_parser.write_yml(cli_output_file)

        self.run(
            stages=stages,
            config_path=cli_args.config_path,
            verbose=(cli_args.verbose != 0),
            output_dir=output_dir,
            cli_args=self._cli_command_parser.cli_args_dict,
        )

    def _update_defaults(self, overide_defaults):
        params_to_update = yaml.safe_load(
            "\n".join(f"{a} : {b}" for a, b in overide_defaults)
        )

        for (path, value) in params_to_update.items():
            self.config_manager.set(path, value)

    def _install_config(self, cli_args):
        """
        Install the config

        Parameters
        ----------
            cli_args: argparse.Namespace
                CLI arguments
        """
        if cli_args.overide_defaults:
            self._update_defaults(cli_args.overide_defaults)
        self.config_manager.write_yml(
            f"{cli_args.config_install_path}/{self.name}.yml"
        )

    def run(
        self,
        output_dir,
        stages=None,
        config_path=None,
        verbose=False,
        cli_args=None,
    ):
        """
        Executes the pipeline

        Parameters
        ----------
          output_dir: str
             Path to output directory
          stages: list[str]
             Names of the stages to be executed
          config_path: str
             Configuration yaml file path
          verbose: bool
             Toggle DEBUG log level
          cli_args: dict
             CLI arguments
        """
        stages = [] if stages is None else stages
        cli_args = {} if cli_args is None else cli_args

        LogUtil.configure(self.name, output_dir=output_dir, verbose=verbose)

        self.logger.info("=============== START =====================")
        self.logger.info(f"Executing {self.name} pipeline with metadata:")
        self.logger.info(f"Infile Path: {cli_args.get('input')}")
        self.logger.info(f"Stages: {stages}")
        self.logger.info(f"Configuration Path: {config_path}")
        self.logger.info(f"Current run output path : {output_dir}")

        executor = ExecutorFactory.get_executor(
            output_dir, name=self.name, **cli_args
        )
        if config_path:
            self.config_manager.update_config(config_path)

        if stages:
            self.config_manager.update_pipeline(
                self._pipeline_config(selected_stages=stages),
            )

        self._stages.validate(self.config_manager.stages_to_run)

        self._stages.update_pipeline_parameters(
            self.config_manager.stages_to_run,
            self.config_manager.parameters,
            _output_dir_=output_dir,
            _cli_args_=cli_args,
            _global_parameters_=self.config_manager.global_parameters,
        )

        executable_stages = self._stages.get_stages(
            self.config_manager.stages_to_run
        )

        self.logger.info(
            f"""Selected stages to run: {', '.join(
                stage.name for stage in executable_stages
            )}"""
        )

        config_output_file = (
            f"{output_dir}/{self.name}_{timestamp()}.config.yml"
        )
        self.config_manager.write_yml(config_output_file)

        self.scheduler.schedule(executable_stages)
        executor.execute(self.scheduler.tasks)

        self.logger.info("=============== FINISH =====================")
