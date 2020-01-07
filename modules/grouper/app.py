from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.target_path as target_path

log = get_logger()


def group(path, out_path):
    """
    Link files into the output directory.
    :param path: File or directory paths.
    :param out_path: The output path.
    """
    for file_path in file_crawler.crawl(path):
        target = target_path.get_path(file_path, out_path)
        log.debug(f'target: {target}')
        file_linker.link(file_path, target)


def main():
    """Group input files without modifying file paths."""
    env = environs.Env()
    data_path = env('DATA_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'data_path: {data_path} out_path: {out_path}')
    group(data_path, out_path)


if __name__ == '__main__':
    main()
