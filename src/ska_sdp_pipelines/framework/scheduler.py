import dask
from dask.distributed import Client

from .log_util import LogUtil


class SchedulerFactory:
    @staticmethod
    def get_scheduler(dask_scheduler=None):
        if dask_scheduler:
            return DaskScheduler(dask_scheduler)

        return DefaultScheduler()


class DefaultScheduler:
    def __init__(self):
        self.delayed_outputs = []

    def schedule(self, selected_stages, vis, config, verbose=False):
        output = None
        for stage in selected_stages:
            kwargs = stage.stage_config.extend(
                **config.stage_config(stage.name)
            )

            pipeline_data = dict()
            pipeline_data["input_data"] = vis
            pipeline_data["output"] = output

            output = dask.delayed(LogUtil.with_log)(
                verbose, stage, pipeline_data, **kwargs
            )
            self.delayed_outputs.append(output)

    def execute(self):
        return dask.compute(*self.delayed_outputs)


class DaskScheduler(DefaultScheduler):
    def __init__(self, dask_scheduler):
        super().__init__()
        self.client = Client(dask_scheduler)
        self.client.forward_logging()
