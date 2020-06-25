#!/usr/bin/env python3
from pathlib import Path
import json
from typing import List

import structlog

log = structlog.get_logger()


class AssetLocationFileParser(object):
    """Class to parse GEOJson format asset location files."""

    def __init__(self, path: Path) -> None:
        with open(str(path), 'r') as file:
            geojson = json.load(file)
            features = geojson['features']
            for feature in features:
                props = feature['properties']
                self.context: List[str] = props['context']

    def contains_context(self, context: str) -> bool:
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

    def matching_context_items(self, match: str) -> List[str]:
        """
        Return context items containing the given string.

        :param match: The string to match.
        :return Context items containing the match.
        """
        matches = []
        if not self.context:
            return matches
        for item in self.context:
            if match in item:
                matches.append(item)
        return matches
