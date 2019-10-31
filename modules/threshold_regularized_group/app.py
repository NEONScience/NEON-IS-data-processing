from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.target_path as target_path

log = get_logger()


def group(regularized_path, threshold_path, out_path):
    for file_path in file_crawler.crawl(regularized_path):
        log.debug(f'regularized file: {file_path.name}')
        target = target_path.get_path(file_path, out_path)
        file_linker.link(file_path, target)
    for file_path in file_crawler.crawl(threshold_path):
        log.debug(f'threshold file: {file_path.name}')
        destination = target_path.get_path(file_path, out_path)
        file_linker.link(file_path, destination)


def main():
    env = environs.Env()
    regularized_path = env('REGULARIZED_PATH')
    threshold_path = env('THRESHOLD_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'regularized_path: {regularized_path} threshold_path: {threshold_path} out_path: {out_path}')
    group(regularized_path, threshold_path, out_path)


if __name__ == '__main__':
    main()
