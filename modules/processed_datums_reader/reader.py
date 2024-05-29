from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs, pps
from collections import defaultdict
from pathlib import Path


def read_processed_files(client: Client, l1_pipelines_path: Path) -> defaultdict[lambda: defaultdict[int]]:
    pipeline_name = "level1_pipeline_list"
    files_by_pipeline = defaultdict(lambda: defaultdict(int))
    for source in l1_pipelines_path.rglob('*'):
        if source.is_file():
            with (client.pfs.pfs_file(file=source) as pfs_file):
                for pipeline_name in pfs_file:
                    pipeline_name = pipeline_name.decode('UTF-8')
                    pipeline_name = pipeline_name.strip()
                    pipeline_info = client.pps.inspect_pipeline(pipeline=pps.Pipeline(name=pipeline_name), details=True)
                    project_name = pipeline_info.pipeline.project.name
                    pipeline_commit_name = f'{project_name}/{pipeline_name}@master'

                    for file in client.pfs.glob_file(commit=pfs.Commit.from_uri(pipeline_commit_name), pattern='/????/??/??/*'):
                        path = file.file.path
                        path_parts = path.split('/')
                        pipeline_date_path = f'/{path_parts[1]}/{path_parts[2]}/{path_parts[3]}'
                        processed_date = f'{path_parts[1]}-{path_parts[2]}-{path_parts[3]}'
                        # print(f'processed_date: {processed_date}')
                        # print(f'pipeline_date_path: {pipeline_date_path}')
                        group_name = path_parts[4]
                        # print(f'group_name: {group_name}')
                        files_by_pipeline[pipeline_name][processed_date] += 1
    return files_by_pipeline
