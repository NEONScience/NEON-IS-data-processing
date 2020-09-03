#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

from common import log_config

from parquet_linkmerge.parquet_file_merger import ParquetFileMerger

log = structlog.get_logger()


def main():
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    # default 30 percent duplication threshold
    duplication_threshold: float = env.float('DEDUPLICATION_THRESHOLD', 0.3)
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    parquet_file_merger = ParquetFileMerger(data_path=in_path,
                                            out_path=out_path,
                                            duplication_threshold=duplication_threshold,
                                            relative_path_index=relative_path_index)
    parquet_file_merger.merge()


if __name__ == '__main__':
    main()
