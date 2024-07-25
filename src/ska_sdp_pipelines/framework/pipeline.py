import functools
import logging

from .configuration import Configuration
from .constants import CONFIG_CLI_ARGS, MANDATORY_CLI_ARGS
from .exceptions import NoStageToExecuteException, StageNotFoundException
from .io_utils import create_output_dir, read_dataset, write_dataset
from .log_util import LogUtil
from .model.cli_command import CLICommand
from .model.config_manager import ConfigManager
from .model.named_instance import NamedInstance
from .scheduler import SchedulerFactory


class Pipeline(metaclass=NamedInstance):
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

        self.name = name
        self._stages = [] if stages is None else stages
        self._global_config = (
            Configuration() if global_config is None else global_config
        )

        self._cli_command = CLICommand()

        self._cli_command.create_sub_parser(
            "run",
            self._run,
            MANDATORY_CLI_ARGS + ([] if cli_args is None else cli_args),
            help="Run the pipeline",
        )

        self._cli_command.create_sub_parser(
            "install-config",
            self._install_config,
            CONFIG_CLI_ARGS,
            help="Installs the default config at --config-install-path",
        )

        LogUtil.configure(name)
        self.logger = logging.getLogger(self.name)

        self.config_manager = ConfigManager(
            pipeline=self._pipeline_config(),
            parameters=self._parameter(),
            global_parameters=self._global_config.items,
        )

    def __validate_stages(self, stages):
        """
        Validates the stages name with the pipeline definition stages.

        Parameters
        ----------
        stages: [str]
            Stages names

        Raises
        ------
        StageNotFoundException
            If any non-existing stage found.
        """
        stages_names = [stage.name for stage in self._stages]

        non_existing_stages = [
            stage for stage in stages if stage not in stages_names
        ]
        if non_existing_stages:
            raise StageNotFoundException(
                f"Stages not found: {non_existing_stages}"
            )

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
            cli_args=self._cli_command.cli_args_dict,
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

    def __call__(self):
        """
        Run the pipeline as a CLI command
        """
        cli_args = self._cli_command.parse_args()
        cli_args.sub_command(cli_args)

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
        self.config_manager.update_config(config_path=config_path)

        if stages:
            self.config_manager.update_pipeline(
                self._pipeline_config(selected_stages=stages),
            )

        self.__validate_stages(stages)

        active_stages = self.config_manager.stages_to_run

        if not active_stages:
            raise NoStageToExecuteException("Selected stages empty")

        executable_stages = []
        for stage in self._stages:
            if stage.name in active_stages:
                stage.update_pipeline_parameters(
                    self.config_manager.stage_config(stage.name),
                    _input_data_=vis,
                    _output_dir_=output_dir,
                    _cli_args_=cli_args,
                    _global_parameters_=self.config_manager.global_parameters,
                )

                executable_stages.append(stage)

        self.logger.info(
            f"""Selected stages to run: {', '.join(
                stage.name for stage in executable_stages
            )}"""
        )

        self.config_manager.write_yml(f"{output_dir}/config.yml")

        scheduler.schedule(executable_stages, verbose=verbose)
        output_pipeline_data = scheduler.execute()

        write_dataset(output_pipeline_data, output_dir)

        self.logger.info("=============== FINISH =====================")
