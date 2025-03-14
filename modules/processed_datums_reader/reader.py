from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs, pps
from collections import defaultdict
from pathlib import Path


def read_processed_files(client: Client, l1_pipelines_path: Path) -> defaultdict[lambda: defaultdict[int]]:
    files_by_pipeline = defaultdict(lambda: defaultdict(int))
    with open(l1_pipelines_path, 'r') as file:
        # Read the content of the file
        line = file.readline()
        while line:
            pipeline_name = line.strip()
            pipeline_info = client.pps.inspect_pipeline(pipeline=pps.Pipeline(name=pipeline_name), details=True)
            project_name = pipeline_info.pipeline.project.name
            pipeline_commit_name = f'{project_name}/{pipeline_name}@master'

            for processed_file in client.pfs.glob_file(commit=pfs.Commit.from_uri(pipeline_commit_name), pattern='/????/??/??/*'):
                path = processed_file.file.path
                path_parts = path.split('/')
                pipeline_date_path = f'/{path_parts[1]}/{path_parts[2]}/{path_parts[3]}'
                processed_date = f'{path_parts[1]}-{path_parts[2]}-{path_parts[3]}'
                group_name = path_parts[4]
                files_by_pipeline[pipeline_name][processed_date] += 1
            line = file.readline()
    return files_by_pipeline
