#!/usr/bin/env python3
from pathlib import Path
import os
import json

from structlog import get_logger

log = get_logger()


def transform(*, related_paths: list, data_path: Path, location_path: Path, out_path: Path, 
                 relative_path_index: int, year_index: int, month_index: int, loc_index: int) -> None:
    """
    :param related_paths: Input paths
    :param data_path: Input data path that defines the output prefix (e.g. month)
    :param out_path: The output path for writing results.
    :param relative_path_index: Trim path components before this index.
    """
    # Store path prefix and output basenames
    input_paths_by_basename = {}
    # Store sites for locations
    site_by_loc = {}
    loc_by_basename = {}
    prefix = None
    for related_path in related_paths:
        # if related_path is a file, pass its parent to the related_path iterator
        if related_path.is_file():
            related_path = related_path.parent
        for path in related_path.rglob('*'):
            if path.is_file():
                basename = os.path.basename(path)
                input_paths_by_basename[basename] = path
                # If this path defines the output prefix, store prefix 
                if (str(related_path) == str(data_path)):              
                    parts = path.parts
                    loc = parts[loc_index]
                    if prefix is None:
                        prefix = parts[year_index:month_index+1]
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
        dest_path = Path(out_path, *prefix, site_by_loc[loc_by_basename[basename]], basename)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if not dest_path.exists():
            dest_path.symlink_to(input_paths_by_basename.get(basename))
