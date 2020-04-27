#!/usr/bin/env python3
import os
import json


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
        self.__parse()

    def __parse(self):
        """
        Read all pipeline specification files in the given path and parse their inputs.

        :return:
        """
        for root, dirs, files in os.walk(self.specification_path):
            for file in files:
                if file.endswith('.json'):
                    specification_file = os.path.join(root, file)
                    pipeline_input_repos = []
                    with open(specification_file) as jsonFile:
                        json_data = json.load(jsonFile)
                        pipeline_name = json_data['pipeline']['name']
                        self.pipeline_files[pipeline_name] = specification_file
                        has_input_repo = False
                        if specification_file == self.end_node_specification:
                            self.end_node_pipeline_name = pipeline_name
                        for key, value in json_data['input'].items():
                            has_input_repo = self.__parse_input(key, value, pipeline_input_repos, has_input_repo)
                        if has_input_repo:
                            self.pipeline_inputs[pipeline_name] = pipeline_input_repos

    def __parse_input(self, key, value, input_repos, has_input_repo=False):
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
                        has_input_repo = self.__parse_input(key3, value3, input_repos, has_input_repo)  # recurse
            if isinstance(value, dict):
                for key3, value3 in value.items():
                    has_input_repo = self.__parse_input(key3, value3, input_repos, has_input_repo)  # recurse
        return has_input_repo

    def get_end_node_pipeline(self):
        return self.end_node_pipeline_name

    def get_pipeline_files(self):
        return self.pipeline_files

    def get_pipeline_inputs(self):
        return self.pipeline_inputs
