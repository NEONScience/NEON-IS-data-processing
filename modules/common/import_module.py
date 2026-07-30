#!/usr/bin/env python3
import importlib.util
import sys
from types import ModuleType


# import modules from a specified path (e.g. from a base image)
def import_base_module(module_name: str, module_path: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load a module spec from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
