#!/usr/bin/env python3
import os
import json
import yaml


class PipelineSpecificationParser(object):

    def __init__(self, end_node_specification, specification_path):
        """
        Constructor.

        :param end_node_specification: path to the pipeline specifications for the endpoint node
        :type end_node_specification: str
        :param specification_path: Path to directory containing pipeline specifications.
        :type specification_path: str
        :return:
        """
        self.end_node_specification = end_node_specification
        self.specification_path = specification_path
        self.end_node_pipeline_name = None
        self.pipeline_inputs = {}
        self.pipeline_files = {}
        self.parse()

    def parse(self):
        """Read all pipeline specification files and parse their inputs."""
        for root, dirs, files in os.walk(self.specification_path):
            for file in files:
                file_path = os.path.join(root, file)
                pipeline_input_repos = []
                if file.endswith('.yaml'):
                    self.parse_yaml(file_path, pipeline_input_repos)
                if file.endswith('.json'):
                    self.parse_json(file_path, pipeline_input_repos)

    def parse_yaml(self, specification_file, pipeline_input_repos):
        """
        Parse a YAML specification file.

        :param specification_file: The file path.
        :param pipeline_input_repos: A list to hold the pipeline inputs.
        :return:
        """
        with open(specification_file) as yaml_file:
            file_data = yaml.load(yaml_file, Loader=yaml.FullLoader)
            self.parse_file_data(specification_file, file_data, pipeline_input_repos)

    def parse_json(self, specification_file, pipeline_input_repos):
        """
        Parse a JSON specification file.

        :param specification_file: The file path.
        :param pipeline_input_repos: A list to hold the pipeline inputs.
        :return:
        """
        with open(specification_file) as json_file:
            file_data = json.load(json_file)
            self.parse_file_data(specification_file, file_data, pipeline_input_repos)

    def parse_file_data(self, specification_file, file_data, pipeline_input_repos):
        """
        Parse the specification file data.

        :param specification_file: The file path.
        :param file_data: The file data.
        :param pipeline_input_repos: A list for appending the pipeline inputs.
        :return:
        """
        pipeline_name = file_data['pipeline']['name']
        self.pipeline_files[pipeline_name] = specification_file
        has_input_repo = False
        if specification_file == self.end_node_specification:
            self.end_node_pipeline_name = pipeline_name
        for key, value in file_data['input'].items():
            has_input_repo = self.parse_pipeline_input(
                key, value, pipeline_input_repos, has_input_repo)
        if has_input_repo:
            self.pipeline_inputs[pipeline_name] = pipeline_input_repos

    def parse_pipeline_input(self, key, value, input_repos, has_input_repo=False):
        """
        Parse an input entry from a Pachyderm pipeline specification.

        :param key: An input repo key should be 'pfs'
        :type key: str
        :param value: The input repo name.
        :param input_repos: The list of input repos to populate.
        :type input_repos: list
        :param has_input_repo: true indicates the pipeline has at least one input.
        :type has_input_repo: bool
        :return:
        """
        if key == 'pfs':
            has_input_repo = True
            repo = value['repo']
            if repo not in input_repos:
                input_repos.append(repo)
        else:
            if isinstance(value, list):
                for value2 in value:
                    for key3, value3 in value2.items():
                        has_input_repo = self.parse_pipeline_input(
                            key3, value3, input_repos, has_input_repo)  # recurse
            if isinstance(value, dict):
                for key3, value3 in value.items():
                    has_input_repo = self.parse_pipeline_input(
                        key3, value3, input_repos, has_input_repo)  # recurse
        return has_input_repo

    def get_end_node_pipeline(self):
        """Return the last pipeline in the DAG.

        :return: str of the pipeline name
        """
        return self.end_node_pipeline_name

    def get_pipeline_files(self):
        """Return all the pipeline files in the DAG.

        :return: dict of files organized by pipeline
        """
        return self.pipeline_files

    def get_pipeline_inputs(self):
        """
        Return inputs by pipeline

        :return: dict of inputs organized by pipeline
        """
        return self.pipeline_inputs
