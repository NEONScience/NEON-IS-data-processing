#!/usr/bin/env python3
from pathlib import Path
import json

import structlog

log = structlog.get_logger()


class AssetLocationFileParser(object):
    """Class to parse GEOJson format asset location files."""

    def __init__(self, path: Path):
        with open(str(path), 'r') as file:
            geojson = json.load(file)
            features = geojson['features']
            for feature in features:
                props = feature['properties']
                self.context = props['context']

    def contains_context(self, context: str):
        """
        Match the context to a location file context.

        :param context: The context to match.
        :return True if file contains the given context.
        """
        if not self.context:
            return False
        for item in self.context:
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
        if not self.context:
            return False
        for item in self.context:
            if match in item:
                matches.append(item)
        return matches
