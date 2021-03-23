#!/usr/bin/env python3
import structlog
from pathlib import Path

log = structlog.get_logger()


class DataGapFillerLinker:
    """
    Class to link files from the input repository into the output repository.
    The input repository is assumed to have a collection of empty files identified
    by a particular suffix (passed in) and regular data files. If data files exist
    in a directory with empty files the data files are linked, otherwise the empty files
    are linked.
    """

    def __init__(self, in_path: Path,  out_path: Path, relative_path_index: int,
                 location_index: int, empty_file_suffix: str) -> None:
        """
        Constructor.

        :param in_path: The input path.
        :param out_path: The output path for linking files and directories.
        :param relative_path_index: Trim input paths up to this index.
        :param location_index: The input path location index.
        :param empty_file_suffix: The file extension for empty files.
        """
        self.in_path = in_path
        self.out_path = out_path
        self.relative_path_index = relative_path_index
        self.location_index = location_index
        self.empty_file_suffix = empty_file_suffix

    def link_files(self) -> None:
        """Link input files and directories into the output path."""
        empty_file_glob_pattern = f'**/*{self.empty_file_suffix}'
        for empty_file_path in self.in_path.glob(empty_file_glob_pattern):
            location_path_parts = empty_file_path.parts[:self.location_index + 1]
            output_location_path = Path(self.out_path, *location_path_parts[self.relative_path_index:])
            if not output_location_path.exists():
                for path in Path(*location_path_parts).rglob('*'):
                    log.debug(f'input path: {path}')
                    parts = path.parts
                    if path.is_dir():
                        # add the metadata directories to the output path in case they are empty
                        dir_path = Path(self.out_path, *parts[self.relative_path_index:])
                        log.debug(f'creating directory: {dir_path}')
                        dir_path.mkdir(parents=True, exist_ok=True)
                    elif path.is_file():
                        # check if the file is an empty file
                        if path.suffix == self.empty_file_suffix:
                            # data files do not have the empty file suffix,
                            # remove suffix with stem to check for a data file
                            data_file_path = Path(*parts[:len(parts) - 1], path.stem)
                            # if a data file exists, do not link the empty file
                            if data_file_path.exists():
                                log.debug(f'data file {data_file_path} exists')
                                continue
                            else:
                                # no data file exists, link the empty file
                                dir_path = Path(self.out_path, *parts[self.relative_path_index:len(parts) - 1])
                                dir_path.mkdir(parents=True, exist_ok=True)
                                link_path = Path(dir_path, path.stem)  # trim the empty file suffix
                                if not link_path.exists():
                                    log.debug(f'linking empty file {path} to {link_path}')
                                    link_path.symlink_to(path)
                        # link the data file
                        else:
                            dir_path = Path(self.out_path, *parts[self.relative_path_index:len(parts) - 1])
                            dir_path.mkdir(parents=True, exist_ok=True)
                            link_path = Path(dir_path, path.name)
                            if not link_path.exists():
                                log.debug(f'linking data file {path} to {link_path}')
                                link_path.symlink_to(path)
