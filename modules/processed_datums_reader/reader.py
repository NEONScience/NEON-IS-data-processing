import os
from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs
from pachyderm_sdk.api.pfs import FileType
from collections import defaultdict


def read_processed_files(client: Client) -> defaultdict(lambda: defaultdict(int)):
    pipeline_name = "level1_pipeline_list"
    input_repo = f"/{pipeline_name}"
    date_pattern = "/????/??/??"  # Adjust this to match your date pattern
    commit_id = "master"
    files_by_pipeline = defaultdict(lambda: defaultdict(int))
    source = (pfs.File.from_uri("level1_pipeline_list@master:/level1_pipeline_list.txt"))
    with (client.pfs.pfs_file(file=source) as pfs_file):
        for pipeline_name in pfs_file:
            data_pattern_path = f'{pipeline_name}@master:/????/??/??'
            date_paths = client.pfs.glob_file(data_pattern_path)
            for processed_date in date_paths:
                group_path_pattern = f'{pipeline_name}@master:{processed_date}/*'
                group_paths = client.pfs.glob_file(group_path_pattern)
                for group in group_paths:
                        files_by_pipeline[pipeline_name][processed_date] += 1
    return files_by_pipeline

