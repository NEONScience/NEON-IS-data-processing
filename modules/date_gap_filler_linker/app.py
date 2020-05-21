#!/usr/bin/env python3
import structlog
import environs
import glob
from pathlib import Path

import lib.log_config as log_config

log = structlog.get_logger()


class Linker(object):

    def __init__(self, in_path, out_path, relative_path_index, empty_file_suffix):
        """
        Constructor.

        :param in_path: The root input path.
        :type in_path: Path
        :param out_path: The output path for linking files and directories.
        :type out_path: Path
        :param relative_path_index: Trim input paths up to this index.
        :type relative_path_index: int
        :param empty_file_suffix: The file extension for empty files.
        :type empty_file_suffix: str
        """
        self.in_path = in_path
        self.out_path = out_path
        self.relative_path_index = relative_path_index
        self.empty_file_suffix = empty_file_suffix

    def link(self):
        """
        Link input files and directories into the output path.

        :return:
        """
        paths = self.in_path.glob('**/*')
        for path in paths:
            log.debug(f'path: {path}')
            parts = path.parts
            if path.is_dir():
                # add the metadata directories to the output path in case they are empty
                dir_path = Path(self.out_path, *parts[self.relative_path_index:])
                log.debug(f'dir_path: {dir_path}')
                dir_path.mkdir(parents=True)
            elif path.is_file():
                # check for empty file
                if path.suffix == self.empty_file_suffix:
                    # real paths do not have the empty file suffix, strip it off with stem
                    real_path = Path(*parts[:len(parts) - 1], path.stem)
                    log.debug(f'real_path: {real_path}')
                    # if a real file exists, do not link the empty file
                    if real_path.exists():
                        log.debug('real path exists')
                        continue
                    else:
                        # no real file exists, trim the suffix to link the empty file
                        # with the expected real file name
                        link_path = Path(self.out_path, *parts[self.relative_path_index:len(parts) - 1], path.stem)
                        log.debug(f'linking {link_path} to {path}')
                        link_path.symlink_to(path)
                else:
                    if self.location_is_active(parts):
                        link_path = Path(self.out_path, *parts[self.relative_path_index:])
                        log.debug(f'linking {link_path} to {path}')
                        link_path.symlink_to(path)

    def location_is_active(self, path_parts):
        """
        Check to see if the directory contains empty files indicating the location was active.

        :param path_parts:
        :return:
        """
        glob_path = Path(self.in_path, *path_parts[self.relative_path_index:len(path_parts) - 2])
        # glob on the data type and filename
        glob_pattern = f'{glob_path}/*/*{self.empty_file_suffix}'
        log.debug(f'glob_pattern: {glob_pattern}')
        if glob.glob(glob_pattern):
            return True
        return False


def main():
    env = environs.Env()
    in_path = env.path('IN_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    empty_file_suffix = env.str('EMPTY_FILE_SUFFIX')

    log_config.configure(log_level)

    linker = Linker(in_path, out_path, relative_path_index, empty_file_suffix)
    linker.link()


if __name__ == '__main__':
    main()
