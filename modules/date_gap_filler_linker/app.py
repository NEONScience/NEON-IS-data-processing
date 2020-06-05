#!/usr/bin/env python3
import structlog
import environs
from pathlib import Path

import common.log_config as log_config

log = structlog.get_logger()


class Linker(object):

    def __init__(self,
                 in_path: Path,
                 out_path: Path,
                 relative_path_index: int,
                 location_index: int,
                 empty_file_suffix: str):
        """
        Constructor.

        :param in_path: The root input path.
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

    def link(self):
        """Link input files and directories into the output path."""
        empty_file_glob_pattern = f'**/*{self.empty_file_suffix}'
        for empty_file_path in self.in_path.glob(empty_file_glob_pattern):
            location_path_parts = empty_file_path.parts[:self.location_index + 1]
            output_location_path = Path(self.out_path, *location_path_parts[self.relative_path_index:])
            if not output_location_path.exists():
                for path in Path(*location_path_parts).glob('**/*'):
                    log.debug(f'input path: {path}')
                    parts = path.parts
                    if path.is_dir():
                        # add the metadata directories to the output path in case they are empty
                        dir_path = Path(self.out_path, *parts[self.relative_path_index:])
                        log.debug(f'creating directory: {dir_path}')
                        dir_path.mkdir(parents=True, exist_ok=True)
                    elif path.is_file():
                        # check if it is an empty file
                        if path.suffix == self.empty_file_suffix:
                            # data files do not have the empty file suffix,
                            # remove suffix with stem to check for a data file
                            data_file_path = Path(*parts[:len(parts) - 1], path.stem)
                            # if a data file exists, do not link the empty file
                            if data_file_path.exists():
                                log.debug(f'data file [{data_file_path}] exists, empty file will not be linked')
                                continue
                            else:
                                # no data file exists, link the empty file
                                dir_path = Path(self.out_path, *parts[self.relative_path_index:len(parts) - 1])
                                dir_path.mkdir(parents=True, exist_ok=True)
                                link_path = Path(dir_path, path.stem)  # trim the empty file suffix
                                log.debug(f'linking empty file [{path}] to [{link_path}]')
                                link_path.symlink_to(path)
                        else:
                            dir_path = Path(self.out_path, *parts[self.relative_path_index:len(parts) - 1])
                            dir_path.mkdir(parents=True, exist_ok=True)
                            link_path = Path(dir_path, path.name)
                            log.debug(f'linking data file [{path}] to [{link_path}]')
                            link_path.symlink_to(path)


def main():
    """Read environment variables and create the Linker object."""
    env = environs.Env()
    in_path = env.path('IN_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    location_index = env.int('LOCATION_INDEX')
    empty_file_suffix = env.str('EMPTY_FILE_SUFFIX')

    log_config.configure(log_level)

    linker = Linker(in_path, out_path, relative_path_index, location_index, empty_file_suffix)
    linker.link()


if __name__ == '__main__':
    main()
