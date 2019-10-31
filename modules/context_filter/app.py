import environs
import structlog

from lib import log_config as log_config
import context_filter.grouper as grouper


def main():
    env = environs.Env()
    in_path = env('IN_PATH')
    out_path = env('OUT_PATH')
    context = env('CONTEXT')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path}')
    log.debug(f'out_path: {out_path}')
    log.debug(f'context: {context}')
    grouper.group(in_path, out_path, context)


if __name__ == '__main__':
    main()
