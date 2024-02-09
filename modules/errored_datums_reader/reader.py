from collections import defaultdict

from pachyderm_sdk import Client
from pachyderm_sdk.api.pfs import FileType


def read_error_files(client: Client) -> defaultdict[str, list]:
    """Read the output repo datums marked with errors and save by repo name."""
    paths_by_repo: defaultdict[str, list[str]] = defaultdict(list)
    for branch_info in client.pfs.list_branch():
        if branch_info.branch.name == 'master':
            repo_name = branch_info.branch.repo.name
            for file_info in client.pfs.glob_file(commit=branch_info.head, pattern='/errored_datums/**'):
                if file_info.file_type == FileType.FILE:
                    path = file_info.file.path
                    paths_by_repo[repo_name] = path
    return paths_by_repo
