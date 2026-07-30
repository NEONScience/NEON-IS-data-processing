#!/usr/bin/env python3
import importlib
from pathlib import Path

import structlog
from array_parser import calibration_file_parser, data_file_parser, schema_parser
from array_parser.array_parser_config import Config
from array_parser.path_parser import PathParser
from array_parser.schema_parser import SchemaData
from cloudpathlib import CloudPath
from common import import_module

log = structlog.get_logger()


def parse(config: Config) -> None:
    data_path: Path | CloudPath = config.data_path
    schema_path: Path = config.schema_path
    out_path: Path = config.out_path
    source_type: str = config.source_type
    data_date: str = config.data_date
    source_type_out: str = config.source_type_out
    replace_schema_name: bool = config.replace_schema_name
    write_site_file: bool = config.write_site_file
    parse_calibration: bool = config.parse_calibration
    test_mode: bool = config.test_mode
    source_id_list_str: str = config.source_id_list
    using_filesystem: bool = config.using_filesystem
    file_version: str = config.file_version
    common_modules_path: str = config.common_path
    utils_path: str = config.utils_path
    trigger_table_update: bool = config.update_trigger_table
    schema_data: SchemaData = schema_parser.parse_schema_file(schema_path)
    parser = PathParser(config)
    files_to_parse = []
    # max date per site for trigger table update
    max_date_per_site = None
    gen_path = (
        import_module.import_base_module(
            "gen_path", common_modules_path + "gen_path.py"
        )
        if common_modules_path
        else None
    )
    neon_avro_kafka_utils = (
        import_module.import_base_module(
            "neon_avro_kafka_utils", utils_path + "__init__.py"
        )
        if utils_path
        else None
    )
    update_trigger_table = importlib.import_module(
        "neon_avro_kafka_utils.update_trigger_table"
    )

    if using_filesystem:
        for path in data_path.rglob("*"):
            if path.is_file():
                files_to_parse.append(path)
    else:
        assert gen_path is not None
        source_id_list = {
            entry.strip() for entry in source_id_list_str.split(",") if entry.strip()
        }
        for source_id in source_id_list:
            source_id_path = gen_path.get_hivestyle_path(
                data_path,
                file_version,
                data_date,
                source_type,
                source_id,
            )
            for path in source_id_path.rglob(gen_path.get_hivestyle_glob(data_date)):
                if path.is_file():
                    files_to_parse.append(path)
        # get max date per site for trigger table update
        max_date_per_site = {}
    for path in files_to_parse:
        if using_filesystem:
            source_type, year, month, day, source_id, data_type = parser.parse(path)
            if source_type_out is not None:
                common_path = Path(
                    out_path, source_type_out, year, month, day, source_id
                )
            else:
                common_path = Path(out_path, source_type, year, month, day, source_id)
            if data_type == "data":
                if test_mode:
                    log.debug(f"Linking file: {path} to {Path(common_path, data_type)}")
                    link_data_file(path, Path(common_path, data_type))
                else:
                    log.debug(f"Parsing file: {path}")
                    data_file_parser.write_restructured_file(
                        path,
                        Path(common_path, data_type),
                        schema_path,
                        replace_schema_name,
                        write_site_file,
                    )
            elif parse_calibration and data_type == "calibration":
                log.debug(f"Parsing calibration file: {path}")
                link_calibration_file(path, Path(common_path, data_type), schema_data)
            else:
                # Copy/link over other files in the directory
                log.debug(f"Linking file: {path} to {Path(common_path, data_type)}")
                link_data_file(path, Path(common_path, data_type))
        else:
            assert gen_path is not None
            out_file = gen_path.create_output_path(
                source_type,
                Path(
                    out_path / file_version,
                    source_type_out,
                    *Path(str(path)).parts[4:-1],
                ),
                rm_offsets=True,
                replace_source_type=False,
            )
            log.debug(f"Parsing file: {path}")
            data_file_parser.write_restructured_file(
                path,
                out_file,
                schema_path,
                replace_schema_name,
                write_site_file,
                max_date_per_site,
                data_date,
            )
    if max_date_per_site and trigger_table_update:
        for site in max_date_per_site:
            log.info(f"Updating trigger table for {site}")
            update_trigger_table.call_update_trigger_table(
                site, source_type_out, max_date=max_date_per_site[site]
            )


def link_calibration_file(path: Path, out_path, schema_data: SchemaData) -> None:
    stream_id = calibration_file_parser.get_stream_id(path)
    field_name = schema_data.calibration_mapping.get(stream_id)
    link_path = Path(out_path, field_name, path.name)
    if not link_path.exists():
        log.debug(f"calibration link: {link_path}")
        link_path.parent.mkdir(parents=True, exist_ok=True)
        link_path.symlink_to(path)


def link_data_file(path: Path, out_path: Path) -> None:
    link_path = Path(out_path, path.name)
    if not link_path.exists():
        link_path.parent.mkdir(parents=True, exist_ok=True)
        log.debug(f"data link: {link_path}")
        link_path.symlink_to(path)
