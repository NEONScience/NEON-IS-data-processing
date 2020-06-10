from pathlib import Path
import glob
import os
import structlog

log = structlog.get_logger()


def walk(path: Path):
    """
    Recursively yield all files in the path.

    :param path: The path to walk.
    :return: All files in the path.
    """
    if path.is_file():
        return path
    for root, directories, files in os.walk(str(path)):
        for file in files:
            file_path = Path(root, file)
            yield file_path


def link(*, path: Path, link_path: Path):
    """
    Symbolically link the given paths.

    :param path: The existing path to be linked.
    :param link_path: The link path to create.
    """
    if not link_path.exists():
        link_path.parent.mkdir(parents=True, exist_ok=True)
        log.debug(f'linking {path} to {link_path}')
        link_path.symlink_to(path)


def link_index(*, path: Path, output_path: Path, path_index: int):
    """Link sub paths of elements beginning at the given index to all files in path."""
    if path.is_file():
        link_path = Path(output_path, sub_path(path=path, path_index=path_index))
        link(path=path, link_path=link_path)
    for root, directories, files in os.walk(str(path)):
        for file in files:
            file_path = Path(root, file)
            link_path = Path(output_path, sub_path(path=file_path, path_index=path_index))
            link(path=file_path, link_path=link_path)


def link_indices(*, path: Path, output_path: Path, path_indices: list):
    """Link sub paths of elements to all files in path."""
    if path.is_file():
        link_path = Path(output_path, sub_path_elements(path=path, path_indices=path_indices))
        link(path=path, link_path=link_path)
    for root, directories, files in os.walk(str(path)):
        for file in files:
            file_path = Path(root, file)
            link_path = Path(output_path, sub_path_elements(path=file_path, path_indices=path_indices))
            link(path=file_path, link_path=link_path)


def sub_path(*, path: Path, path_index: int):
    """
    Return a new path of path elements beginning at the given index.

    :param path_index: The index to begin extracting path elements.
    :param path: The source path to extract elements.
    :return: A new path of the path elements after the index.
    """
    new_path = Path(*path.parts[path_index:])
    return new_path


def sub_path_elements(*, path: Path, path_indices: list):
    """
    Return a new path of indexed path elements.

    :param path_indices: The indices of path elements to include in the new path.
    :param path: The source path to extract elements.
    :return: A new path made from the elements at the given indices.
    """
    new_path = Path()
    parts = path.parts
    for index in path_indices:
        part = parts[index]
        new_path = new_path.joinpath(part)
    return new_path


def filter_files(*, glob_pattern: str, output_path: Path):
    """
    Filter input files from the filesystem according to the given Unix style path glob pattern.

    :param glob_pattern: The path pattern to match.
    :param output_path: Path to ignore.
    :return File paths matching the given glob pattern.
    """
    files = [file_path for file_path in glob.glob(glob_pattern, recursive=True)
             if not os.path.basename(file_path).startswith(str(output_path))
             if os.path.isfile(file_path)]
    return files
