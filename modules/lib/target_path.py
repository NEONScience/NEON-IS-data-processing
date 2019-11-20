import os
import pathlib


def get_path(source_path, out_path):
    """
    Remove  <root>/<repo name> from the path and prepend the output directory
    to the target path.
    :param source_path: The source file path.
    :param out_path: The output directory to be prepended.
    :return: The full output path.
    """
    path = pathlib.Path(source_path)
    trimmed_path = trim_path(path)
    return os.path.join(out_path, trimmed_path)


def trim_path(path):
    """
    Trim off root and repo name from input directory paths.
    :param path: A full input path
    :return: The path without the root and repo name elements.
    """
    trimmed_path = pathlib.Path(*path.parts[3:])  # Remove first two path elements
    return trimmed_path
