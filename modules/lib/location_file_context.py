#!/usr/bin/env python3
import json
import structlog

log = structlog.get_logger()


def match(location_file, context_match):
    """
    Match the context to a location file context.

    :param location_file: A location file path to load.
    :type location_file: str
    :param context_match: The context to match.
    :type context_match: str
    :return True if file: contains an entry for the given context.
    """
    with open(location_file) as f:
        geojson = json.load(f)
        features = geojson['features']
        for feature in features:
            props = feature['properties']
            context = props['context']
            if not context:
                return False
            for item in context:
                log.debug(f'searching for context: {context_match} in {item}')
                if item == context_match:
                    return True
        return False


def get_matching_items(location_file, context_match):
    """
    Find the given substring in a location file context.

    :param location_file: A location file path to load.
    :type location_file: str
    :param context_match: The context string to find.
    :type context_match: str
    :return The context item containing the matching string.
    """
    matches = []
    with open(location_file) as f:
        geojson = json.load(f)
        features = geojson['features']
        for feature in features:
            props = feature['properties']
            context = props['context']
            if not context:
                return False
            for item in context:
                if context_match in item:
                    matches.append(item)
    return matches
