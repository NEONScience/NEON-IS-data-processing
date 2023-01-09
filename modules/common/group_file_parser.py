import geojson
from pathlib import Path
from typing import List, Tuple


def parse_group_file(path: Path) -> Tuple[str, List[dict], List[str]]:
    """
    Parse a group file.

    :param path: The file path.
    :return: The member name, groups, active periods, HOR, VER
    """
    with open(str(path), 'r') as file:
        geojson_data = geojson.load(file)
        features = geojson_data['features']
        name: List[str] = []
        group: List[str] = []
        hor: List[str] = []
        ver: List[str] = []
        active_periods: List = []
        for feature in features:
            hor.append(feature['HOR'])
            ver.append(feature['VER'])
            props = feature['properties']
            name.append(props['name'])
            group.append(props['group'])
            active_periods_list: List[dict] = props['active_periods']
            active_periods.append(active_periods_list)
        return name, group, active_periods, hor, ver

def get_group(path: Path) -> List[str]:
    """
    Parse the groups from a group file.

    :param path: The file path.
    :return: The file groups.
    """
    group: List[str] = []
    with open(str(path), 'r') as file:
        geojson_data = geojson.load(file)
        features = geojson_data['features']
        for feature in features:
            props = feature['properties']
            group.append(props['group'])
    return group

def get_group_matches(group: List[str], match: str) -> List[str]:
    """
    Return group items containing the given string.

    :param group: The group.
    :param match: The string to match.
    :return group items containing the match.
    """
    matches = []
    if not group:
        return matches
    for item in group:
        if match in item:
            matches.append(item)
    return matches
