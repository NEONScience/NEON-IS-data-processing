from collections import defaultdict

from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs


def read_error_files(client: Client) -> defaultdict[str, list]:
    """Read the files in the error directories and save the paths by pipeline name."""
    files_by_pipeline: defaultdict[str, list[str]] = defaultdict(list)
    for pipeline in client.pps.list_pipeline():
        commit_name = f'{pipeline.pipeline.project.name}/{pipeline.pipeline.name}@master'
        for file in client.pfs.glob_file(commit=pfs.Commit.from_uri(commit_name), pattern='/errored_datums/**'):
            files_by_pipeline[pipeline.pipeline.name] = file.file.path
    return files_by_pipeline
