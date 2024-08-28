import networkx as nx

from ska_sdp_piper.piper.stage.stages import Stages


class GraphStages(Stages):
    def __init__(self, graph=None):
        super().__init__(list(graph.nodes))
        self.graph = graph

    def get_stages(self, stage_names=None):
        """
        Returns the executable stages for the pipeline

        Parameters
        ----------
            stage_names: list(str)
                Stage names of stages to be returned
        Returns
        -------
            list(Stage)
        """
        stages_to_remove = [
            stage for stage in self.graph if stage.name not in stage_names
        ]
        for stage_to_remove in stages_to_remove:
            if stage_to_remove in self.graph:
                dfs_tree = nx.dfs_tree(self.graph, stage_to_remove)
                self.graph.remove_nodes_from(dfs_tree.nodes)

        return self.graph
