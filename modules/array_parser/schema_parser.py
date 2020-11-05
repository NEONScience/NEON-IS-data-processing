#!/usr/bin/env python3
import json
from pathlib import Path
from typing import NamedTuple


class TermMapping(NamedTuple):
    source: str
    mapping: dict


def parse_schema(path: Path) -> TermMapping:
    """
    Parse the context from a location file.

    :param path: The file path.
    :return: The source and field mappings.
    """
    with open(str(path), 'r') as file:
        json_data = json.load(file)
        source = json_data['source']
        fields = json_data['fields']
        mapping = {}
        for field in fields:
            name = field['name']
            if name.startswith('depth'):
                stream_id = field['__neon_stream_id']
                mapping[stream_id] = name
    term_mapping = TermMapping(source=source, mapping=mapping)
    return term_mapping
