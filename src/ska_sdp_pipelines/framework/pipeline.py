import logging
import shutil
from functools import reduce

from .constants import MANDATORY_CLI_ARGS
from .exceptions import NoStageToExecuteException, StageNotFoundException
from .io_utils import create_output_dir, read_dataset, write_dataset, write_yml
from .log_util import LogUtil
from .model.cli_arguments import CLIArguments
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
        self._global_config = global_config
        self._cli_args = CLIArguments(
            MANDATORY_CLI_ARGS + ([] if cli_args is None else cli_args)
        )

        LogUtil.configure(name)
        self.logger = logging.getLogger(self.name)

    @property
    def config(self):
        """
        Pipeline configuration dictionary

        Returns
        -------
            Dictionary containing the stage states and default parameters
        """
        stages_config = reduce(
            lambda config, stage: {**config, **stage.config}, self._stages, {}
        )

        stage_states = {stage.name: True for stage in self._stages}

        return {"pipeline": stage_states, "parameters": stages_config}

    def run(self):
        """
        Run the pipeline as a CLI command
        """
        cli_args = self._cli_args.parse_args()

        stages = [] if cli_args.stages is None else cli_args.stages[0]

        self(
            cli_args.input,
            stages=stages,
            dask_scheduler=cli_args.dask_scheduler,
            config_path=cli_args.config_path,
            verbose=(cli_args.verbose != 0),
            output_path=cli_args.output_path,
            cli_args=self._cli_args.get_cli_args(),
        )

    def __call__(
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
          cli_args: Optional[argparse.Namespace]
             CLI arguments
        """

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
        config = ConfigManager()
        stage_names = [stage.name for stage in self._stages]
        selected_stages = self._stages
        stages_to_run = None
        scheduler = SchedulerFactory.get_scheduler(dask_scheduler)

        if config_path is not None:
            ConfigManager.init(config_path)
            config = ConfigManager.get_config()
            stages_to_run = config.stages_to_run
            shutil.copy(config_path, f"{output_dir}/config.yml")
        else:
            write_yml(f"{output_dir}/config.yml", self.config)

        if stages:
            non_existent_stages = [
                stage_name
                for stage_name in stages
                if stage_name not in stage_names
            ]

            if non_existent_stages:
                raise StageNotFoundException(
                    f"Stages not found: {non_existent_stages}"
                )

            stages_to_run = stages

        if stages_to_run is not None:
            selected_stages = [
                stage for stage in self._stages if stage.name in stages_to_run
            ]
        if not selected_stages:
            raise NoStageToExecuteException("Selected stages empty")

        self.logger.info(
            f"""Selected stages to run: {', '.join(
                stage.name for stage in selected_stages
            )}"""
        )

        scheduler.schedule(
            selected_stages,
            vis,
            config,
            output_dir,
            verbose,
            cli_args=cli_args,
        )

        output_pipeline_data = scheduler.execute()
        write_dataset(output_pipeline_data, output_dir)

        self.logger.info("=============== FINISH =====================")
