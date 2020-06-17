#!/usr/bin/env python3
from pathlib import Path
import json

import structlog

log = structlog.get_logger()


class AssetLocationFileParser(object):

    def __init__(self, path: Path):
        self.path = path

    def contains_context(self, context: str):
        """
        Match the context to a location file context.

        :param context: The context to match.
        :return True if file contains the given context.
        """
        with open(str(self.path), 'r') as file:
            geojson = json.load(file)
            features = geojson['features']
            for feature in features:
                props = feature['properties']
                file_context = props['context']
                if not file_context:
                    return False
                for item in file_context:
                    if item == context:
                        return True
            return False

    def matching_context_items(self, match: str):
        """
        Return context items containing the given string.

        :param match: The string to match.
        :return Context items containing the match.
        """
        matches = []
        with open(str(self.path), 'r') as file:
            geojson_data = json.load(file)
            features = geojson_data['features']
            for feature in features:
                props = feature['properties']
                context = props['context']
                if not context:
                    return False
                for item in context:
                    if match in item:
                        matches.append(item)
        return matches
