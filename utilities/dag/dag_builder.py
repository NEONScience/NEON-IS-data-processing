#!/usr/bin/env python3
from graphviz import Digraph


class DagBuilder(object):

    def __init__(self, end_node_pipeline: str, files_by_pipeline: dict, inputs_by_pipeline: dict):
        """
        Constructor.

        :param end_node_pipeline: The DAG end node pipeline name.
        :param files_by_pipeline: All specification files organized by pipeline name.
        :param inputs_by_pipeline: Repo inputs organized by pipeline name.
        """
        self.files_by_pipeline = files_by_pipeline
        self.inputs = inputs_by_pipeline
        # Populated with all nodes for the DAG (pipelines and source repos).
        self.nodes = []
        # Populated with pipeline specification files for the DAG.
        self.dag_pipeline_files = []
        self.dag_pipelines = []  # Populated with pipeline names for the DAG.
        self.source_repos = {}
        self.dag = Digraph()
        self._build(end_node_pipeline)

    def _build(self, pipeline: str):
        """
        Populate the DAG from the given pipeline end node.

        :param pipeline: The end node for the pipeline.
        """
        if not self.nodes:  # if first call and no nodes exist, add the pipeline as a DAG node
            self.dag.node(pipeline, label=pipeline, shape='box')
            self.nodes.append(pipeline)
            self.dag_pipelines.append(pipeline)
            specification_file = self.files_by_pipeline[pipeline]
            self.dag_pipeline_files.append(specification_file)
        # loop over pipeline inputs
        for input_pipeline in self.inputs[pipeline]:
            if input_pipeline in self.inputs:  # pipeline is part of this DAG
                shape = 'box'
                if 'data_source' not in input_pipeline:  # exclude data source repos
                    self.dag_pipelines.append(input_pipeline)
                    specification_file = self.files_by_pipeline[input_pipeline]
                    self.dag_pipeline_files.append(specification_file)
                    print(f'input pipeline: {input_pipeline}')
            else:  # a source repository, do not include in pipeline files
                shape = 'oval'
                print(f'source repo: {input_pipeline}')
                specification_file = self.files_by_pipeline[input_pipeline]
                self.source_repos.update({'pipeline': input_pipeline, 'file': specification_file})
            if input_pipeline not in self.nodes:
                self.dag.node(input_pipeline,
                              label=input_pipeline, shape=shape)
                self.nodes.append(input_pipeline)
            self.dag.edge(input_pipeline, pipeline)
            if input_pipeline in self.inputs:
                self._build(input_pipeline)
        del self.inputs[pipeline]  # omit duplicate renderings

    def get_dag(self) -> Digraph:
        return self.dag

    def get_pipeline_names(self) -> list:
        return self.dag_pipelines

    def get_pipeline_files(self) -> list:
        return self.dag_pipeline_files

    def get_source_repos(self) -> dict:
        return self.source_repos

    def render(self, output_file: str):
        """
        Render the dag.

        :param output_file: The path of the output file to write.
        """
        self.dag.render(output_file, view=True)
