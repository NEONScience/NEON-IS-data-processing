import geojson
from pathlib import Path
from typing import List, Tuple


def parse_location_file(path: Path) -> Tuple[str, List[dict], List[str]]:
    """
    Parse a location file.

    :param path: The file path.
    :return: The location name, active periods, and context.
    """
    with open(str(path), 'r') as file:
        geojson_data = geojson.load(file)
        features = geojson_data['features']
        properties = features[0]['properties']
        name: str = properties['name']
        active_periods: List[dict] = properties['active_periods']
        context: List[str] = properties['context']
    return name, active_periods, context


def get_context(path: Path) -> List[str]:
    """
    Parse the context from a location file.

    :param path: The file path.
    :return: The file context.
    """
    with open(str(path), 'r') as file:
        geojson_data = geojson.load(file)
        features = geojson_data['features']
        for feature in features:
            props = feature['properties']
            context: List[str] = props['context']
    return context


def get_context_matches(context: List[str], match: str) -> List[str]:
    """
    Return context items containing the given string.

    :param context: The context.
    :param match: The string to match.
    :return Context items containing the match.
    """
    matches = []
    if not context:
        return matches
    for item in context:
        if match in item:
            matches.append(item)
    return matches
