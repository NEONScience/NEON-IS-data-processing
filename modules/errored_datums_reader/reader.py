from collections import defaultdict

from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs
from pachyderm_sdk.api.pfs import FileType


def read_error_files(client: Client) -> defaultdict[str, list]:
    """Read the files in the error directories and save the paths by pipeline name."""
    files_by_pipeline: defaultdict[str, list] = defaultdict(list)
    for pipeline in client.pps.list_pipeline():
        pipeline_name = pipeline.pipeline.name
        commit_name = f'{pipeline.pipeline.project.name}/{pipeline_name}@master'
        for file in client.pfs.glob_file(commit=pfs.Commit.from_uri(commit_name), pattern='/errored_datums/**'):
            if file.file_type == FileType.FILE:
                file_path = file.file.path
                files_by_pipeline[pipeline_name].append(file_path)
    return files_by_pipeline
