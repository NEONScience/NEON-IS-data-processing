#!/usr/bin/env python3
from graphviz import Digraph


class DagBuilder:

    def __init__(self, end_node_pipeline: str, pipeline_files: dict, pipeline_inputs: dict):
        """
        Constructor.

        :param end_node_pipeline: The DAG end node pipeline name.
        :param pipeline_files: All specification files organized by pipeline name.
        :param pipeline_inputs: Repo inputs organized by pipeline name.
        """
        self.pipeline_files = pipeline_files
        self.pipeline_inputs = pipeline_inputs
        self.dag_pipeline_files = []
        self.dag_pipeline_names = []
        self.dag_nodes = []
        self.source_repos = {}
        self.dag = Digraph()
        self.build(end_node_pipeline)

    def build(self, end_node_pipeline: str):
        """
        Populate the DAG from the given pipeline end node.

        :param end_node_pipeline: The end node for the pipeline.
        """
        # add the end node pipeline to the DAG
        if not self.dag_nodes:
            self.dag.node(end_node_pipeline, label=end_node_pipeline, shape='box')
            self.dag_nodes.append(end_node_pipeline)
            self.dag_pipeline_names.append(end_node_pipeline)
            specification_file = self.pipeline_files[end_node_pipeline]
            self.dag_pipeline_files.append(specification_file)
        # loop over pipeline inputs
        for input_pipeline in self.pipeline_inputs[end_node_pipeline]:
            specification_file = self.pipeline_files.get(input_pipeline)
            # pipeline is part of this DAG
            if input_pipeline in self.pipeline_inputs:
                shape = 'box'
                # exclude data source repos
                if 'data_source' not in input_pipeline:
                    self.dag_pipeline_names.append(input_pipeline)
                    self.dag_pipeline_files.append(specification_file)
            # pipeline is a source repository, do not include in DAG pipeline files
            else:
                shape = 'oval'
                print(f'source repo: {input_pipeline}')
                self.source_repos.update({'pipeline': input_pipeline, 'file': specification_file})
            if input_pipeline not in self.dag_nodes:
                self.dag.node(input_pipeline, label=input_pipeline, shape=shape)
                self.dag_nodes.append(input_pipeline)
            self.dag.edge(input_pipeline, end_node_pipeline)
            if input_pipeline in self.pipeline_inputs:
                # recurse this function
                self.build(input_pipeline)
        # omit duplicate renderings
        del self.pipeline_inputs[end_node_pipeline]

    def get_dag(self) -> Digraph:
        return self.dag

    def get_pipeline_names(self) -> list:
        return self.dag_pipeline_names

    def get_pipeline_files(self) -> list:
        return self.dag_pipeline_files

    def get_source_repos(self) -> dict:
        return self.source_repos

    def render(self, output_file: str):
        """
        Render the dag.

        :param output_file: The output file path.
        """
        self.dag.render(output_file, view=True)
