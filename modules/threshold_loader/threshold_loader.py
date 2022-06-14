#!/usr/bin/env python3
from pathlib import Path
import json
from typing import Callable,Iterator

from data_access.types.threshold import Threshold


def load_thresholds(get_thresholds: Callable[[str],Iterator[Threshold]],out_path: Path,term: str,ctxt: str) -> None:
    """
    Write a threshold file into the output path.

    :param get_thresholds: Function yielding thresholds.
    :param out_path: The path for writing results.
    :param term: Same as term_name.
    :param ctxt: Same as context_code.
    """
    with open(Path(out_path,'thresholds.json'),'w') as file:
        thresholds = []
        ctxt_l = ctxt.split("|")
        for threshold in get_thresholds(term=term,ctxt=ctxt):
            if (threshold[3] != []):
                for elem in ctxt_l:
                    if (elem in threshold[3]):
                        thresholds.append(threshold._asdict())
        threshold_data = {}
        threshold_data.update({'thresholds': thresholds})
        json_data = json.dumps(threshold_data,indent=4,sort_keys=False,default=str)
        file.write(json_data)
