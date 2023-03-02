#!/usr/bin/env python3
from pathlib import Path
from typing import Callable


def load_readme(get_readme: Callable[[], str], out_path: Path):
    """
    Write a readme file into the output path.

    :param get_readme: Function returning the "README" template content as a string.
    :param out_path: The path for writing the file.
    """
    with open(Path(out_path, get_filename()), 'w') as file:
        readme_content: str = get_readme()
        file.write(readme_content)


def get_filename() -> str:
    return 'readme-template.txt'

