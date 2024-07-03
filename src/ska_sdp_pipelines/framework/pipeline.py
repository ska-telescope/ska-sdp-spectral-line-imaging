from functools import reduce

import dask

from .exceptions import StageNotFoundException
from .io_utils import create_output_name, read_dataset, write_dataset
from .model.config_manager import ConfigManager
from .pipeline_datastore import PipelineDatastore


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

    def execute_stage(self, stage, pipeline_data, *args, **kwargs):
        """
        Executes individual stages with the pipeline data
        Parameter:
            stage(Stage): Pipeline Stage
            pipeline_data(PipelineDatastore): Rich object wrapping input data
                and previous stage output
        """
        pipeline_data["output"] = stage(pipeline_data, *args, **kwargs)
        return pipeline_data

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

    def __call__(self, infile_path, stages=None, config_path=None):
        """
        Executes the pipeline
        Parameters:
          infile_path (str) : Path to input file
        """

        vis = read_dataset(infile_path)
        pipeline_data = PipelineDatastore(vis)
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

        for stage in selected_satges:
            pipeline_data = dask.delayed(self.execute_stage)(
                stage, pipeline_data, **config.stage_config(stage.name)
            )

        output_pipeline_data = pipeline_data.compute()
        outfile = create_output_name(infile_path, self.name)
        write_dataset(output_pipeline_data, outfile)

    @classmethod
    def get_instance(cls):
        return cls.__instance
