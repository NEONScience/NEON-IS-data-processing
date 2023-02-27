#!/usr/bin/env python3
from contextlib import closing
from functools import partial
from pathlib import Path

from structlog import get_logger
import environs

from common import log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from readme_generator.file_descriptions import get_descriptions
from readme_generator.location_geometry import get_geometry
from readme_generator.log_entry import get_log_entries
from readme_generator.data_product import get_data_product
from readme_generator.data_product_keyword import get_keywords
from readme_generator.generator import generate_readme, Config


def main() -> None:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    template_path: Path = env.str('TEMPLATE_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'out_path: {out_path}')
    config = Config(
        in_path=in_path,
        out_path=out_path,
        template_path=template_path)
    db_config = read_from_mount(Path('/var/db_secret'))
    with closing(DbConnector(db_config)) as connector:
        get_data_product_partial = partial(get_data_product, connector=connector)
        get_log_entries_partial = partial(get_log_entries, connector=connector)
        get_geometry_partial = partial(get_geometry, connector=connector)
        get_descriptions_partial = partial(get_descriptions, connector=connector)
        get_keywords_partial = partial(get_keywords, connector=connector)
        generate_readme(config=config,
                        get_log_entries=get_log_entries_partial,
                        get_data_product=get_data_product_partial,
                        get_geometry=get_geometry_partial,
                        get_descriptions=get_descriptions_partial,
                        get_keywords=get_keywords_partial)


if __name__ == '__main__':
    main()
