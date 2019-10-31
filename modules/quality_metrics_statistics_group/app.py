from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.target_path as target_path

log = get_logger()


def group(statistics_path, quality_path, out_path):
    for file_path in file_crawler.crawl(statistics_path):
        log.debug(f'statistics file: {file_path.name}')
        target = target_path.get_path(file_path, out_path)
        file_linker.link(file_path, target)
    for file_path in file_crawler.crawl(quality_path):
        log.debug(f'quality file: {file_path.name}')
        destination = target_path.get_path(file_path, out_path)
        file_linker.link(file_path, destination)


def main():
    env = environs.Env()
    statistics_path = env('STATISTICS_PATH')
    quality_path = env('QUALITY_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f' statistics_path: {statistics_path} quality_path: {quality_path} out_path: {out_path}')
    group(statistics_path, quality_path, out_path)


if __name__ == '__main__':
    main()
