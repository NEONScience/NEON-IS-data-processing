#!/usr/bin/env python3
from graphviz import Digraph


class DagBuilder(object):

    def __init__(self, end_node_pipeline, files_by_pipeline, inputs_by_pipeline):
        """
        Constructor.

        :param end_node_pipeline: The DAG end node pipeline name.
        :type end_node_pipeline: str
        :param files_by_pipeline: All specification files organized by pipeline name.
        :type files_by_pipeline: dict
        :param inputs_by_pipeline: Repo inputs organized by pipeline name.
        :type inputs_by_pipeline: dict
        """
        self.files_by_pipeline = files_by_pipeline
        self.inputs = inputs_by_pipeline
        self.nodes = []  # Populated with all nodes for the DAG (pipelines and source repos).
        self.dag_pipeline_files = []  # Populated with pipeline specification files for the DAG.
        self.dag_pipelines = []  # Populated with pipeline names for the DAG.
        self.dag = Digraph()
        self.__build(end_node_pipeline)

    def __build(self, pipeline):
        """
        Populate the DAG from the given pipeline end node.

        :param pipeline: The end node for the pipeline.
        :type pipeline: str
        :return:
        """
        if not self.nodes:  # if first call and no nodes exist, add the pipeline
            self.dag.node(pipeline, label=pipeline, shape='box')
            self.nodes.append(pipeline)
            self.dag_pipelines.append(pipeline)
            specification_file = self.files_by_pipeline[pipeline]
            self.dag_pipeline_files.append(specification_file)
        for input_pipeline in self.inputs[pipeline]:  # loop over pipeline inputs
            if input_pipeline in self.inputs:  # is pipeline-generated
                shape = 'box'
                if 'data_source' not in input_pipeline:  # exclude data source repos
                    self.dag_pipelines.append(input_pipeline)
                    specification_file = self.files_by_pipeline[input_pipeline]
                    self.dag_pipeline_files.append(specification_file)
                    print(f'pipeline: {input_pipeline}')
            else:  # not pipeline generated, do not include in pipeline files
                shape = 'oval'
                print(f'source repo: {input_pipeline}')
            if input_pipeline not in self.nodes:
                self.dag.node(input_pipeline, label=input_pipeline, shape=shape)
                self.nodes.append(input_pipeline)
            self.dag.edge(input_pipeline, pipeline)
            if input_pipeline in self.inputs:
                self.__build(input_pipeline)
        del self.inputs[pipeline]  # omit duplicate renderings

    def get_dag(self):
        return self.dag

    def get_pipeline_names(self):
        return self.dag_pipelines

    def get_pipeline_files(self):
        return self.dag_pipeline_files

    def render(self, output_file):
        """
        Render the dag.

        :param output_file: The path of the output file to write.
        :type output_file: str
        :return:
        """
        self.dag.render(output_file, view=True)
