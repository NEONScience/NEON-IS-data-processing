from pathlib import Path
from functools import singledispatch
import glob
import os
import structlog

log = structlog.get_logger()


class PathTransformer(object):

    def __init__(self, input_path: Path, root_output_path: Path):
        self.input_path = input_path
        self.root_output_path = root_output_path
        self.transform = singledispatch(self.transform)
        self.transform.register(int, self._transform_int)
        self.transform.register(list, self._transform_list)
        self.transform_path = singledispatch(self.transform_path)
        self.transform_path.register(int, self._transform_path_int)
        self.transform_path.register(list, self._transform_path_list)

    def crawl(self):
        """
        Yield all files in the input path.

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
        :param link_path: The link path to create by prepending with the root output path.
        """
        link_path = Path(self.root_output_path, link_path)
        link_path.parent.mkdir(parents=True, exist_ok=True)
        if not link_path.exists():
            log.debug(f'linking {link_path} to {path}')
            link_path.symlink_to(path)

    def filter_files(self, glob_pattern: str):
        """
        Filter input files from the filesystem according to the given
        Unix style path glob pattern while ignoring any files in the given output path.

        :param glob_pattern: The path pattern to contains_match.
        :return File paths matching the given glob pattern.
        """
        files = [file_path for file_path in glob.glob(glob_pattern, recursive=True)
                 if not os.path.basename(file_path).startswith(str(self.root_output_path))
                 if os.path.isfile(file_path)]
        return files

    def transform(self, arg):
        """
        Overloaded method for specific argument types.

        :param arg: Placeholder argument.
        """
        pass

    def _transform_int(self, index: int):
        """
        Create links from the path elements past the index for all files in the input path.

        :param index: The index to begin extracting path elements.
        """
        for path in self.crawl():
            link_path = self.transform_path(index, path)
            self.link(path, link_path)

    def _transform_list(self, indices: list):
        """
        Create links from the indexed elements for all files in the input path.

        :param indices: The path elements to extract to create links.
        """
        for path in self.crawl():
            link_path = self.transform_path(indices, path)
            self.link(path, link_path)

    @staticmethod
    def transform_path(arg, path: Path):
        """
        Overloaded method for specific argument types.

        :param arg: Placeholder argument.
        :param path: The source path to extract elements.
        :return: A new path of the extracted elements.
        """
        pass

    @staticmethod
    def _transform_path_int(index: int, path: Path):
        """
        Return a new path of all path elements past the given index.

        :param index: The index to begin extracting path elements.
        :param path: The source path to extract elements.
        :return:
        """
        new_path = Path(*path.parts[index:])
        return new_path

    @staticmethod
    def _transform_path_list(indices: list, path: Path):
        """
        Return a new path consisting of path elements at each given index.

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
