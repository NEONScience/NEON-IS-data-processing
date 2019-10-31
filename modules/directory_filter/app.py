import os

import environs
import structlog

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.target_path as target_path


def filter_directory(in_path, filter_dir, out_path):
    """
    Link the target directory into the output directory.
    """
    for r, d, f in os.walk(in_path):
        for name in d:
            if not name.startswith('.') and name == filter_dir:
                source = os.path.join(r, name)
                destination = target_path.get_path(source, out_path)
                file_linker.link(source, destination)


def main():
    env = environs.Env()
    in_path = env('IN_PATH')
    filter_dir = env('FILTER_DIR')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} sub_dir: {filter_dir} out_dir: {out_path}')
    filter_directory(in_path, filter_dir, out_path)


if __name__ == '__main__':
    main()
