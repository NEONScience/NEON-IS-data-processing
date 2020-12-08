#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

from common import log_config

from parquet_linkmerge.parquet_file_merger import ParquetFileMerger
from parquet_linkmerge.parquet_linkmerge_config import Config

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    # default 30 percent duplication threshold
    duplication_threshold: float = env.float('DEDUPLICATION_THRESHOLD', 0.3)
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    month_index: int = env.int('MONTH_INDEX')
    day_index: int = env.int('DAY_INDEX')
    source_id_index: int = env.int('SOURCE_ID_INDEX')
    log_config.configure(log_level)
    config = Config(in_path=in_path,
                    out_path=out_path,
                    duplication_threshold=duplication_threshold,
                    source_type_index=source_type_index,
                    year_index=year_index,
                    month_index=month_index,
                    day_index=day_index,
                    source_id_index=source_id_index)
    parquet_file_merger = ParquetFileMerger(config)
    parquet_file_merger.merge()


if __name__ == '__main__':
    main()
