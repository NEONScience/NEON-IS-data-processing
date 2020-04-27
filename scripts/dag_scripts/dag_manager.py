#!/usr/bin/env python3
import os

from pipeline_specification_parser import PipelineSpecificationParser
from dag_builder import DagBuilder


class DagManager(object):

    def __init__(self, end_node_specification, specifications_directory):
        """
        Constructor.

        :param end_node_specification: The end node pipeline of the DAG.
        :type end_node_specification: str
        :param specifications_directory: A directory path containing the DAG pipeline specifications.
        :type specifications_directory: str
        """
        parser = PipelineSpecificationParser(end_node_specification, specifications_directory)
        end_node_pipeline = parser.get_end_node_pipeline()
        files_by_pipeline = parser.get_pipeline_files()
        inputs_by_pipeline = parser.get_pipeline_inputs()
        self.dag_builder = DagBuilder(end_node_pipeline, files_by_pipeline, inputs_by_pipeline)

    def create_dag(self):
        """
        Create a DAG from the given pipeline specification files.

        :return:
        """
        pipeline_files = self.dag_builder.get_pipeline_files()  # pipeline specifications from end node to root nodes
        pipeline_files.reverse()  # must create from root nodes to end node
        for pipeline_file in pipeline_files:
            print(f'creating pipeline: {pipeline_file}')
            os.system(f'pachctl create pipeline -f {pipeline_file}')

    def delete_dag(self):
        """
        Delete a DAG beginning from the end node to the root nodes.

        :return:
        """
        for pipeline in self.dag_builder.get_pipeline_names():
            print(f'deleting pipeline: {pipeline}')
            os.system(f'pachctl delete pipeline {pipeline}')

    def graph_dag(self):
        """
        Display a PDF of the DAG.

        :return:
        """
        output_file = 'pipeline-graph'
        self.dag_builder.render(output_file)
        os.remove(output_file)
