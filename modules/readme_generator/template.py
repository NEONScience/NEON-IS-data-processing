#!/usr/bin/env python3
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template


def render(template_path: Path, data) -> str:
    print(f'\ntemplate_path: {template_path}\n')
    environment: Environment = Environment(
        loader=FileSystemLoader(template_path.parent),
        trim_blocks=True,
        lstrip_blocks=True
    )
    template: Template = environment.get_template(template_path.name)
    return template.render(data)
