#!/usr/bin/env python3
import json
from pathlib import Path
from typing import NamedTuple, List


class SchemaData(NamedTuple):
    schema: str
    source_type: str
    field_names: List[str]
    parse_field_names: List[str]
    calibration_mapping: dict
    data_mapping: dict


def parse_schema_file(path: Path) -> SchemaData:
    """
    Get the mapping between stream IDs and schema field names for any applicable calibration data
    Also get the mapping between schema field names and array names

    :param path: The file path.
    :return: The source name and the mapping between stream IDs -> schema field names, and schema field names -> array names (i.e. which array they are in)
    """
    field_exclusions = ['source_id', 'site_id', 'readout_time'] # Assumes all other fields are fields to be parsed. 
    with open(str(path), 'r') as file:
        json_data = json.load(file)
        source_type = json_data['source']
        fields = json_data['fields']
        calibration_mapping = {}
        data_mapping = {}
        field_names = []
        parse_field_names = []
        for field in fields:
            name = field['name']
            field_names.append(name)
            if name not in field_exclusions:
                parse_field_names.append(name)
                try:
                    stream_id = field['__neon_stream_id']
                    array_name = field['__raw_array_name']
                    calibration_mapping[stream_id] = name
                    data_mapping[name] = array_name
                except KeyError:
                    continue
        schema = json.dumps(json_data)
    return SchemaData(schema=schema, source_type=source_type, field_names=field_names, parse_field_names=parse_field_names, calibration_mapping=calibration_mapping,data_mapping=data_mapping)
