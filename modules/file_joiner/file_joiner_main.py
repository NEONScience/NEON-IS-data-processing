#!/usr/bin/env python3
import environs

import common.log_config as log_config

from file_joiner.file_joiner import FileJoiner


def main():
    env = environs.Env()
    config = env.str('CONFIG')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)

    file_joiner = FileJoiner(config=config, out_path=out_path, relative_path_index=relative_path_index)
    file_joiner.join_files()


if __name__ == '__main__':
    main()
