from functools import reduce

import dask
from dask.distributed import Client

from .exceptions import NoStageToExecuteException, StageNotFoundException
from .io_utils import create_output_name, read_dataset, write_dataset
from .model.config_manager import ConfigManager


class Pipeline:
    """
    Pipeline class allows for defining a pipeline as an ordered list of
    stages, and takes care of executing those stages.
    Attributes:
      name (str): Name of the pipeline
      _stage (Stage): Stage to be executed
    """

    __instance = None

    def __init__(self, name, stages=None):
        """
        Initialise the pipeline object
        Parameters:
          name (str) : Name of the pipeline
          stage (Stage) : Stage to be executed
        """
        self.name = name
        self._stages = [] if stages is None else stages
        Pipeline.__instance = self

    def __execute_selected_stages(
        self, selected_stages, vis, config, dask_scheduler=None
    ):
        """
        Executes individual stages with the pipeline data
        Parameter:
            selected_stages([functions]): Wrapped stage functions
            vis(xradio.ps): Input visibilities
            config(ConfigManager): External provided configuration
            dask_scheduler(str): Url to the dask scheduler
        Returns:
            Dask delayed objects
        """
        if dask_scheduler:
            Client(dask_scheduler)
        pipeline_data = dict()
        pipeline_data["input_data"] = vis

        delayed_outputs = []
        for stage in selected_stages:
            kwargs = stage.stage_config.extend(
                **config.stage_config(stage.name)
            )

            output = dask.delayed(stage)(pipeline_data, **kwargs)
            pipeline_data["output"] = output
            delayed_outputs.append(output)

        return delayed_outputs

    @property
    def config(self):
        """
        Pipeline configuration dictionary
        Returns:
            Dictionary containing the stage states and default parameters
        """
        stages_config = reduce(
            lambda config, stage: {**config, **stage.config}, self._stages, {}
        )

        stage_states = {stage.name: True for stage in self._stages}

        return {"pipeline": stage_states, "parameters": stages_config}

    def __call__(
        self, infile_path, stages=None, dask_scheduler=None, config_path=None
    ):
        """
        Executes the pipeline
        Parameters:
          infile_path (str): Path to input file
          stages([str]): Names of the stages to be executed
          dask_scheduler(str): Url of the dask scheduler
        """

        vis = read_dataset(infile_path)
        config = ConfigManager()
        stage_names = [stage.name for stage in self._stages]
        selected_satges = self._stages
        stages_to_run = None

        if config_path is not None:
            ConfigManager.init(config_path)
            config = ConfigManager.get_config()
            stages_to_run = config.stages_to_run

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
            selected_satges = [
                stage for stage in self._stages if stage.name in stages_to_run
            ]
        if not selected_satges:
            raise NoStageToExecuteException("Selected stages empty")

        delayed_output = self.__execute_selected_stages(
            selected_satges, vis, config, dask_scheduler
        )

        output_pipeline_data = dask.compute(*delayed_output)
        outfile = create_output_name(infile_path, self.name)
        write_dataset(output_pipeline_data, outfile)

    @classmethod
    def get_instance(cls):
        return cls.__instance
