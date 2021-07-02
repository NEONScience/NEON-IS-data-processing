#!/usr/bin/env python3
from pathlib import Path
import os
import json

from structlog import get_logger

log = get_logger()


def transform(*, related_paths: list, data_path: Path, location_path: Path, out_path: Path, 
                 relative_path_index: int, year_index: int, loc_index: int) -> None:
    """
    :param related_paths: input paths
    :param data_path: input data path
    :param location_path: input location path
    :param out_path: output path
    :param relative_path_index: trim path components before this index
    :param year_index: index of year in data path
    :param loc_index: index of location in data path
    """
    # Enable input path lookup by file basename
    input_paths_by_basename = {}
    # Enable site lookup by file basename
    site_by_loc = {}
    loc_by_basename = {}
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
                # If this is a location file, store its site
                if (str(related_path) == str(location_path)):
                    parts = path.parts
                    loc = parts[relative_path_index+1]
                    f = open(str(path))
                    loc_data = json.load(f)
                    site = loc_data["features"][0]["properties"]["site"]
                    site_by_loc[loc] = site
                    f.close()
                loc_by_basename[basename] = loc

    # Link each input file into its output path
    for basename in input_paths_by_basename.keys():
        dest_path = Path(out_path, site_by_loc[loc_by_basename[basename]], *yyyymmdd, basename)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if not dest_path.exists():
            dest_path.symlink_to(input_paths_by_basename.get(basename))
