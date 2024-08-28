import dask
import networkx as nx

from ska_sdp_piper.piper.scheduler import DefaultScheduler
from ska_sdp_piper.piper.utils.log_util import LogUtil


class GraphScheduler(DefaultScheduler):
    def __init__(self, *args, **kwargs):
        super().__init__()

    def schedule(self, graph, verbose=False):
        # output = None
        levels = list(nx.topological_generations(graph))
        level_output_dict = {}
        for level in levels:
            for stage in level:
                output = {
                    p.name: level_output_dict[p.name]
                    for p in graph.predecessors(stage)
                }
                stage_output = dask.delayed(LogUtil.with_log)(
                    verbose,
                    stage,
                    output,
                )
                level_output_dict[stage.name] = stage_output
                self.delayed_outputs.append(stage_output)
