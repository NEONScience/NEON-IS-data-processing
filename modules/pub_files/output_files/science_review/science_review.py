from pathlib import Path

from pub_files.database.file_variables import FileVariables, get_is_science_review


def write_file() -> Path:
    file_variables: FileVariables = get_is_science_review()
    pass
