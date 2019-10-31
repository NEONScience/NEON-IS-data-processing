import environs
import structlog

from lib import log_config as log_config
import data_calibration_group.grouper as grouper


def main():
    env = environs.Env()
    data_path = env('DATA_PATH')
    calibration_path = env('CALIBRATION_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'data_path: {data_path}')
    log.debug(f'calibration_path: {calibration_path}')
    log.debug(f'out_path: {out_path}')
    grouper.group(data_path, calibration_path, out_path)


if __name__ == '__main__':
    main()
