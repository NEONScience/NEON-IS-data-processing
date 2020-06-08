from pathlib import Path
import glob
import os
import structlog

log = structlog.get_logger()


class FileRepository(object):

    def __init__(self, input_path: Path, output_path: Path):
        self.input_path = input_path
        self.output_path = output_path

    def crawl(self):
        """
        Recursively yield all files in the input path.

        :return: All files in the path.
        """
        if self.input_path.is_file():
            return self.input_path
        for root, directories, files in os.walk(str(self.input_path)):
            for file in files:
                yield Path(root, file)

    def link(self, path: Path, link_path: Path):
        """
        Symbolically link the given paths.

        :param path: The existing path to be linked.
        :param link_path: The link path to create by prepending the output path.
        """
        link_path = Path(self.output_path, link_path)
        if not link_path.exists():
            link_path.parent.mkdir(parents=True, exist_ok=True)
            log.debug(f'linking {link_path} to {path}')
            link_path.symlink_to(path)

    def filter(self, glob_pattern: str) -> list:
        """
        Filter input files from the filesystem according to the given Unix style path glob pattern.

        :param glob_pattern: The path pattern to contains_match.
        :return File paths matching the given glob pattern.
        """
        files = [file_path for file_path in glob.glob(glob_pattern, recursive=True)
                 if not os.path.basename(file_path).startswith(str(self.output_path))
                 if os.path.isfile(file_path)]
        return files

    def link_all(self, index: int):
        """
        Create links from path elements beginning at the index for all files.

        :param index: The index to begin extracting path elements.
        """
        for path in self.crawl():
            link_path = self.sub_path(path, index)
            self.link(path, link_path)

    def link_all_elements(self, indices: list):
        """
        Create links from the index path elements for all files.

        :param indices: The path elements to extract to create links.
        """
        for path in self.crawl():
            link_path = self.sub_path_elements(path, indices)
            self.link(path, link_path)

    @staticmethod
    def sub_path(path: Path, index: int):
        """
        Return a new path of path elements beginning at the given index.

        :param index: The index to begin extracting path elements.
        :param path: The source path to extract elements.
        :return: A new path of the path elements after the index.
        """
        new_path = Path(*path.parts[index:])
        return new_path

    @staticmethod
    def sub_path_elements(path: Path, indices: list):
        """
        Return a new path of indexed path elements.

        :param indices: The indices of path elements to include in the new path.
        :param path: The source path to extract elements.
        :return: A new path made from the elements at the given indices.
        """
        new_path = Path()
        parts = path.parts
        for index in indices:
            part = parts[index]
            new_path = new_path.joinpath(part)
        return new_path
