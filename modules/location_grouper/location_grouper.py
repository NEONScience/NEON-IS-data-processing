#!/usr/bin/env python3
from pathlib import Path
import os
import json

from structlog import get_logger

log = get_logger()


def location_group(*, related_paths: list, data_path: Path, location_path: Path, out_path: Path, 
                 relative_path_index: int, year_index: int, loc_index: int, grouploc_key: str) -> None:
    """
    :param related_paths: input paths
    :param data_path: input data path
    :param location_path: input location path
    :param out_path: output path
    :param relative_path_index: trim path components before this index
    :param year_index: index of year in data path
    :param loc_index: index of location in data path
    :param grouploc_key: identifier for group location (e.g. "site")
    """
    # Enable input path lookup by file basename
    input_paths_by_basename = {}
    # Enable location group lookup by file basename
    grouploc_by_loc = {}
    loc_by_basename = {}
    prefix_by_basename = {}
    yyyymmdd = None
    for related_path in related_paths:
        # if related_path is a file, pass its parent to the related_path iterator
        if related_path.is_file():
            related_path = related_path.parent
        for path in related_path.rglob('*'):
            if path.is_file():
                basename = os.path.basename(path)
                input_paths_by_basename[basename] = path
                # If this is a data file, store yyyymmdd
                if (str(related_path) == str(data_path)):              
                    parts = path.parts
                    loc = parts[loc_index]
                    if yyyymmdd is None:
                        yyyymmdd = parts[year_index:year_index+3]
                    prefix_by_basename[basename] = 'data' + os.path.sep + loc
                    loc_by_basename[basename] = loc
                # If this is a location file, store its grouploc
                if (str(related_path) == str(location_path)):
                    parts = path.parts
                    loc = parts[relative_path_index+1]
                    f = open(str(path))
                    loc_data = json.load(f)
                    grouploc = loc_data["features"][0]["properties"][grouploc_key]
                    grouploc_by_loc[loc] = grouploc
                    f.close()
                    prefix_by_basename[basename] = 'locations' + os.path.sep + loc
                    loc_by_basename[basename] = loc

    # Link each input file into its output path
    for basename in input_paths_by_basename.keys():
        dest_path = Path(out_path, grouploc_by_loc[loc_by_basename[basename]], *yyyymmdd, prefix_by_basename[basename], basename)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if not dest_path.exists():
            print("LINKING " + basename)
            dest_path.symlink_to(input_paths_by_basename.get(basename))
