import functools
import logging

from .command import Command
from .configuration import Configuration
from .constants import CONFIG_CLI_ARGS, MANDATORY_CLI_ARGS
from .io_utils import create_output_dir, read_dataset, timestamp, write_dataset
from .log_util import LogUtil
from .model.config_manager import ConfigManager
from .model.named_instance import NamedInstance
from .model.stages import Stages
from .scheduler import SchedulerFactory


class Pipeline(Command, metaclass=NamedInstance):
    """
    Pipeline class allows for defining a pipeline as an ordered list of
    stages, and takes care of executing those stages.

    Attributes
    ----------
      name: str
          Name of the pipeline
      _stages: list[ConfigurableStage]
          Stage to be executed
    """

    def __init__(
        self, name, stages=None, global_config=None, cli_args=None, **kwargs
    ):
        """
        Initialise the pipeline object

        Parameters
        ----------
          name: str
              Name of the pipeline
          stages: list[ConfigurableStage]
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

        LogUtil.configure(name)
        self.logger = logging.getLogger(self.name)

        self._stages = Stages(stages)

        self.config_manager = ConfigManager(
            pipeline=self._pipeline_config(),
            parameters=self._parameter(),
            global_parameters=self._global_config.items,
        )

        self.sub_command(
            "run",
            MANDATORY_CLI_ARGS + ([] if cli_args is None else cli_args),
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

        self.run(
            cli_args.input,
            stages=stages,
            dask_scheduler=cli_args.dask_scheduler,
            config_path=cli_args.config_path,
            verbose=(cli_args.verbose != 0),
            output_path=cli_args.output_path,
            cli_args=self._cli_command_parser.cli_args_dict,
        )

    def _install_config(self, cli_args):
        """
        Install the config
        Parameters
        ----------
            cli_args: argparse.Namespace
                CLI arguments
        """
        self.config_manager.write_yml(
            f"{cli_args.config_install_path}/{self.name}.yml"
        )

    def run(
        self,
        infile_path,
        stages=None,
        dask_scheduler=None,
        config_path=None,
        verbose=False,
        output_path=None,
        cli_args=None,
    ):
        """
        Executes the pipeline

        Parameters
        ----------
          infile_path : str
             Path to input file
          stages: list[str]
             Names of the stages to be executed
          dask_scheduler: str
             Url of the dask scheduler
          config_path: str
             Configuration yaml file path
          verbose: bool
             Toggle DEBUG log level
          output_path: str
             Path to root output directory
          cli_args: dict
             CLI arguments
        """
        stages = [] if stages is None else stages
        if output_path is None:
            output_path = "./output"
        output_dir = create_output_dir(output_path, self.name)

        LogUtil.configure(self.name, output_dir=output_dir, verbose=verbose)

        self.logger.info("=============== START =====================")
        self.logger.info(f"Executing {self.name} pipeline with metadata:")
        self.logger.info(f"Infile Path: {infile_path}")
        self.logger.info(f"Stages: {stages}")
        self.logger.info(f"Dask scheduler: {dask_scheduler}")
        self.logger.info(f"Configuration Path: {config_path}")
        self.logger.info(f"Current run output path : {output_dir}")

        vis = read_dataset(infile_path)

        scheduler = SchedulerFactory.get_scheduler(dask_scheduler)
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
            _input_data_=vis,
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

        output_yaml_file = f"{output_dir}/{self.name}_{timestamp()}.config.yml"
        self.config_manager.write_yml(output_yaml_file)

        scheduler.schedule(executable_stages, verbose=verbose)
        output_pipeline_data = scheduler.execute()

        write_dataset(output_pipeline_data, output_dir)

        self.logger.info("=============== FINISH =====================")
