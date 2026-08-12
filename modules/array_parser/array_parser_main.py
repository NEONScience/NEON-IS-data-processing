#!/usr/bin/env python3
from pathlib import Path

import environs
import structlog
from array_parser import array_parser
from array_parser.array_parser_config import Config
from cloudpathlib import CloudPath
from common import import_module, log_config

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    common_path: str | None = env.str("COMMON_PATH", "app/common/", None)
    utils_path: str | None = env.str("UTILS_PATH", "app/neon_avro_kafka_utils/", None)
    if not common_path:
        data_path: Path = env.path("DATA_PATH")
    else:
        gen_path = import_module.import_base_module(
            "gen_path", common_path + "gen_path.py"
        )
        data_path: Path | CloudPath = gen_path.get_path(env.str("DATA_PATH"))
    schema_path: Path = env.path("SCHEMA_PATH")
    out_path: Path = env.path("OUT_PATH")
    source_type: str | None = env.str("SOURCE_TYPE", None)
    data_date: str | None = env.str("DATA_DATE", None)
    source_id_list: str | None = env.str("SOURCE_ID_LIST", None)
    parse_calibration = env.bool("PARSE_CALIBRATION", False)
    log_level: str = env.str("LOG_LEVEL", "INFO")
    source_type_index: int | None = env.int("SOURCE_TYPE_INDEX", None)
    source_type_out: str = env.str("SOURCE_TYPE_OUT", None)
    replace_schema_name: str = env.bool("REPLACE_SCHEMA_NAME", True)
    write_site_file: str = env.bool("WRITE_SITE_FILE", False)
    year_index: int | None = env.int("YEAR_INDEX", None)
    month_index: int | None = env.int("MONTH_INDEX", None)
    day_index: int | None = env.int("DAY_INDEX", None)
    source_id_index: int | None = env.int("SOURCE_ID_INDEX", None)
    data_type_index: int | None = env.int("DATA_TYPE_INDEX", None)
    test_mode: bool = env.bool("TEST_MODE", False)
    using_filesystem: bool = env.bool("USING_FILESYSTEM", True)
    file_version: str | None = env.str("FILE_VERSION", "v2")
    common_path: str | None = env.str("COMMON_PATH", "app/common/", None)
    utils_path: str | None = env.str("UTILS_PATH", "app/neon_avro_kafka_utils/", None)
    update_trigger_table: bool | None = env.bool("UPDATE_TRIGGER_TABLE", False)
    log_config.configure(log_level)
    log.debug(f"data_path: {data_path} schema_path: {schema_path} out_path: {out_path}")
    config = Config(
        data_path=data_path,
        schema_path=schema_path,
        out_path=out_path,
        source_type=source_type,
        data_date=data_date,
        source_id_list=source_id_list,
        parse_calibration=parse_calibration,
        source_type_index=source_type_index,
        source_type_out=source_type_out,
        replace_schema_name=replace_schema_name,
        write_site_file=write_site_file,
        year_index=year_index,
        month_index=month_index,
        day_index=day_index,
        source_id_index=source_id_index,
        data_type_index=data_type_index,
        test_mode=test_mode,
        using_filesystem=using_filesystem,
        file_version=file_version,
        common_path=common_path,
        utils_path=utils_path,
        update_trigger_table=update_trigger_table,
    )
    array_parser.parse(config)


if __name__ == "__main__":
    main()
