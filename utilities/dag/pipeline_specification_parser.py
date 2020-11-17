#!/usr/bin/env python3
import json
import yaml
from pathlib import Path


class PipelineSpecificationParser:

    def __init__(self, end_node_specification: Path, specification_path: Path):
        """
        Constructor.

        :param end_node_specification: Path to the specifications for the endpoint node
        :param specification_path: Path to directory containing specifications.
        """
        self.end_node_specification = end_node_specification
        self.specification_path = specification_path
        self.end_node_pipeline_name = None
        self.pipeline_inputs = {}
        self.pipeline_files = {}
        self.parse_all()

    def parse_all(self):
        """Read all specification files and parse their inputs."""
        for path in self.specification_path.rglob('*'):
            if path.is_file():
                if path.suffix == '.yaml':
                    self.parse_yaml(path)
                if path.suffix == '.json':
                    self.parse_json(path)

    def parse_yaml(self, path: Path):
        with open(str(path)) as yaml_file:
            print(f'loading YAML: {path}')
            file_data = yaml.load(yaml_file, Loader=yaml.FullLoader)
            self.parse_file_data(path, file_data)

    def parse_json(self, path: Path):
        with open(str(path)) as json_file:
            print(f'loading JSON: {path}')
            file_data = json.load(json_file)
            self.parse_file_data(path, file_data)

    def parse_file_data(self, path: Path, file_data: dict):
        """
        Parse the file data.

        :param path: The file path.
        :param file_data: The file data.
        """
        pipeline_name = file_data['pipeline']['name']
        print(f'adding {path} to pipeline: {pipeline_name}')
        self.pipeline_files[pipeline_name] = path

        pipeline_has_inputs = False
        if path.samefile(self.end_node_specification):
            self.end_node_pipeline_name = pipeline_name
        # list to accumulate all the inputs for this pipeline file
        pipeline_input_repos = []
        inputs = file_data.get('input')
        if inputs is not None:
            for key, value in inputs.items():
                pipeline_has_inputs = self.parse_pipeline_input(
                    key, value, pipeline_input_repos, pipeline_has_inputs)
            if pipeline_has_inputs:
                self.pipeline_inputs[pipeline_name] = pipeline_input_repos

    def parse_pipeline_input(self, key: str, value, pipeline_input_repos: list, has_input_repo=False):
        """
        Parse the inputs from a specification file.

        :param key: An input repo key, should be 'pfs'.
        :param value: The input repo name(s).
        :param pipeline_input_repos: The list of input repos to populate.
        :param has_input_repo: true indicates the pipeline has at least one input.
        """
        if key == 'pfs':
            has_input_repo = True
            repo = value['repo']
            if repo not in pipeline_input_repos:
                pipeline_input_repos.append(repo)
        else:
            if isinstance(value, list):
                for value2 in value:
                    for key3, value3 in value2.items():
                        # recurse
                        has_input_repo = self.parse_pipeline_input(
                            key3, value3, pipeline_input_repos, has_input_repo)
            if isinstance(value, dict):
                for key3, value3 in value.items():
                    # recurse
                    has_input_repo = self.parse_pipeline_input(
                        key3, value3, pipeline_input_repos, has_input_repo)
        return has_input_repo

    def get_end_node_pipeline(self) -> str:
        """
        Return the last pipeline in the DAG.

        :return: The pipeline name.
        """
        return self.end_node_pipeline_name

    def get_pipeline_files(self) -> dict:
        """
        Return all the pipeline files in the DAG.

        :return: Files organized by pipeline.
        """
        return self.pipeline_files

    def get_pipeline_inputs(self) -> dict:
        """
        Return inputs organized by pipeline

        :return: Inputs organized by pipeline
        """
        return self.pipeline_inputs
