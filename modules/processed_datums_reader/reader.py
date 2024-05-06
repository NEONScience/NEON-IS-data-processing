import os
from collections import defaultdict

from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs
from pachyderm_sdk.api.pfs import FileType
from collections import defaultdict



def read_processed_files(client: Client) -> defaultdict[str, list]:
    """Read the files in the error directories and save the paths by pipeline name."""
    #files_by_group: defaultdict[str, list] = defaultdict(list)
    files_by_pipeline = defaultdict(lambda: defaultdict(int))
    for pipeline in client.pps.list_pipeline():
        glob_pattern = "/????/??/??/**"
        pipeline_name = pipeline.pipeline.name
        commit_name = f'{pipeline.pipeline.project.name}/{pipeline_name}@master'
        file_count = 0
        for file in client.list_file(commit=pfs.Commit.from_uri(commit_name), pattern=glob_pattern):
            if file.file_type == FileType.FILE:
                path_components = file.split(os.sep)
                if len(path_components) >= 4:
                    # Return the fourth component (index 3, as indexing starts from 0)
                    group_name = path_components[3]
                    files_by_pipeline[pipeline_name][group_name] += 1
    return files_by_pipeline

