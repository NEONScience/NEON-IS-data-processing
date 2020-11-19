#!/usr/bin/env python3
import json
from pathlib import Path
from typing import NamedTuple, List


class SchemaData(NamedTuple):
    schema: str
    source_type: str
    field_names: List[str]
    mapping: dict


def parse_schema_file(path: Path) -> SchemaData:
    """
    Get the mapping between stream IDs and schema field names.

    :param path: The file path.
    :return: The source name and the mapping between stream IDs and schema field names.
    """
    field_exclusions = ['source_id', 'site_id', 'readout_time']
    with open(str(path), 'r') as file:
        json_data = json.load(file)
        source_type = json_data['source']
        fields = json_data['fields']
        mapping = {}
        field_names = []
        for field in fields:
            name = field['name']
            if name not in field_exclusions:
                field_names.append(name)
                try:
                    stream_id = field['__neon_stream_id']
                    mapping[stream_id] = name
                except KeyError:
                    continue
        schema = json.dumps(json_data)
    return SchemaData(schema=schema, source_type=source_type, field_names=field_names, mapping=mapping)
