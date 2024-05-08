import os
from collections import defaultdict

from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs
from pachyderm_sdk.api.pfs import FileType
from collections import defaultdict



def read_processed_files(client: Client) -> defaultdict[str, list]:
    """Read the files in the error directories and save the paths by pipeline name."""
    #files_by_group: defaultdict[str, list] = defaultdict(list)
    repo_name = "level1_pipeline_list"
    file_path = "level1_pipeline_list.txt"
    file_bytes = client.get_file_bytes(repo_name, file_path)
    file_content = file_bytes.decode('utf-8')
    files_by_pipeline = defaultdict(lambda: defaultdict(int))
    for pipeline_name in io.StringIO(file_content):
        #for pipeline in client.pps.list_pipeline():
        glob_pattern = "/????/??/??/**"
        date_pattern = "/????/??/??"
        #pipeline_name = pipeline.pipeline.name
        commit_name = f'{pipeline.pipeline.project.name}/{pipeline_name}@master'
        file_count = 0
        for file in client.pfs.glob_file(commit=pfs.Commit.from_uri(commit_name), pattern=date_pattern):
            if file.file_type == FileType.FILE:
                file_path = file.file.path
                path_components = file_path.split(os.sep)
                if len(path_components) >= 4:
                    # Return the fourth component (index 3, as indexing starts from 0)
                    group_name = path_components[4]
                    files_by_pipeline[pipeline_name][group_name] += 1
    return files_by_pipeline

