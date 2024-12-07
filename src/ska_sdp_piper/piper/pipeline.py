import functools
import logging

from .command import Command
from .configurations import Configuration
from .configurations.runtime_config import RuntimeConfig
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
        self._global_config = global_config or Configuration()
        self.logger = logging.getLogger(self.name)
        self._stages = stages
        self.scheduler = scheduler

        self.sub_command(
            "run",
            DEFAULT_CLI_ARGS + (cli_args or []),
            help="Run the pipeline",
        )(self._run)

        self.sub_command(
            "install-config",
            CONFIG_CLI_ARGS,
            help="Installs the default config at --config-install-path",
        )(self._install_config)

    def _pipeline_config(self):
        """
        Returns the pipeline config as a dictionary of enabled/disabled stages.

        Returns
        -------
        dict
            Dictionary of selected stages
        """

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
        return dict(
            pipeline=self._pipeline_config(),
            parameters=self._parameter(),
            global_parameters=self._global_config.items,
        )

    def _install_config(
        self, config_install_path, override_defaults=None, **kwargs
    ):
        """
        Install the config.
        This can be run from cli as "install-config" subcommand.

        Parameters
        ----------
            config_install_path: str
                Directory to install config to.
            override_defaults: list[list[str, any]], optional
                A list of parameters to override in the config.
                Each element of the list is a list containing key
                to update and the new value.
        """
        override_defaults = override_defaults or []

        RuntimeConfig(**self.config).update_from_cli_overrides(
            override_defaults
        ).write_yml(f"{config_install_path}/{self.name}.yml")

    def _run(
        self,
        config_path=None,
        output_path=None,
        stages=None,
        dask_scheduler=None,
        override_defaults=None,
        with_report=False,
        verbose=0,
        **kwargs,
    ):
        """
        Run the pipeline.
        This can be run from cli as "run" subcommand.

        Parameters
        ----------
            stages: list[str], optional
                A list containing names of the stages to execute.
            override_defaults: list[list[str, any]], optional
                A list of parameters to override in the config
                at runtime.

        Returns
        -------
            None
        """
        stages = stages or []
        override_defaults = override_defaults or []
        verbose = verbose != 0

        output_dir = output_path or "./output"

        timestamped_output_dir = create_output_dir(output_dir, self.name)

        log_file = f"{timestamped_output_dir}/{self.name}_{timestamp()}.log"
        LogUtil.configure(log_file, verbose=verbose)

        runtime_config = (
            RuntimeConfig(**self.config)
            .update_from_yaml(config_path)
            .update_from_cli_overrides(override_defaults)
            .update_from_cli_stages(stages)
        )

        self._stages.update_stage_parameters(runtime_config.parameters)
        self._global_config.update_config_params(
            **runtime_config.global_parameters
        )

        cli_output_file = (
            f"{timestamped_output_dir}/{self.name}_{timestamp()}.cli.yml"
        )
        self._cli_command_parser.write_yml(cli_output_file)

        config_output_path = (
            f"{timestamped_output_dir}/{self.name}_{timestamp()}.config.yml"
        )
        runtime_config.write_yml(config_output_path)

        self.logger.info("=============== START =====================")
        self.logger.info(f"Executing {self.name} pipeline with metadata:")
        self.logger.info(f"Stages: {stages}")
        self.logger.info(f"Configuration Path: {config_path}")
        self.logger.info(f"Current run output path : {timestamped_output_dir}")

        executor = ExecutorFactory.get_executor(
            dask_scheduler=dask_scheduler,
            output_dir=timestamped_output_dir,
            verbose=verbose,
            with_report=with_report,
        )

        stages = runtime_config.stages_to_run
        self._stages.validate(stages)

        self._stages.add_additional_parameters(
            _output_dir_=timestamped_output_dir,
            _cli_args_=kwargs,
            _global_parameters_=self._global_config.items,
        )

        executable_stages = self._stages.get_stages(stages)

        self.logger.info(
            f"""Selected stages to run: {', '.join(
                stage.name for stage in executable_stages
            )}"""
        )

        self.scheduler.schedule(executable_stages)
        self.logger.info("Scheduling done, now executing the graph...")

        executor.execute(self.scheduler.tasks)

        self.logger.info("=============== FINISH =====================")
