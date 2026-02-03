#!/usr/bin/env python3
from pathlib import Path
import json
from typing import Callable, Iterator, List

from data_access.types.threshold import Threshold


def load_thresholds(get_thresholds: Callable[[str], Iterator[Threshold]], out_path: Path, term: str, contexts: List[str]):
    """
    Write a threshold file into the output path with combined results from multiple context sets.

    :param get_thresholds: Function yielding thresholds.
    :param out_path: The path for writing results.
    :param term: The term name.
    :param contexts: List of context strings (each with pipe-separated values).
    """
    # Create output directory if it doesn't exist
    out_path.mkdir(parents=True, exist_ok=True)
    
    with open(Path(out_path,'thresholds.json'), 'w') as file:
        all_thresholds = []
        seen = set()  # Track unique thresholds to avoid duplicates
        
        # Get all thresholds once (generator, so materialize it to a list)
        all_db_thresholds = list(get_thresholds(term=term))
        
        # Process each context set
        for context in contexts:
            context_l = context.split("|") if context != 'none' else []
            
            # Filter thresholds by this context
            for threshold in all_db_thresholds:
                threshold_contexts = threshold[3] if threshold[3] else []
                
                # If no context filter, or if context filter matches
                if not context_l or set(context_l).issubset(set(threshold_contexts)):
                    # Create unique key to avoid duplicates
                    unique_key = (threshold[0], threshold[1], threshold[2])  # threshold_name, term_name, location_name
                    if unique_key not in seen:
                        seen.add(unique_key)
                        all_thresholds.append(threshold._asdict())
        
        threshold_data = {'thresholds': all_thresholds}
        json_data = json.dumps(threshold_data, indent=4, sort_keys=True, default=str)
        file.write(json_data)
