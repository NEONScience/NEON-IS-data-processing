#!/usr/bin/env python3
import importlib.util
import sys
from types import ModuleType


# import modules from a specified path (e.g. from a base image)
def import_base_module(module_name: str, module_path: str) -> ModuleType:
    from pathlib import Path

    module_path_obj = Path(module_path)
    is_package = module_path_obj.name == "__init__.py"
    spec = importlib.util.spec_from_file_location(
        module_name,
        module_path,
        submodule_search_locations=[str(module_path_obj.parent)] if is_package else None,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load a module spec from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
