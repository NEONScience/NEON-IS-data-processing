#!/usr/bin/env python3
import structlog
import logging
import sys
from typing import Union


def configure(log_level: Union[int, str]) -> None:
    """
    Configures logging to write JSON to standard output.

    :param log_level: The log level to set.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    handler = logging.StreamHandler(sys.stdout)
    root_logger.addHandler(handler)
    processors = [structlog.stdlib.filter_by_level,
                  structlog.stdlib.add_logger_name,
                  structlog.stdlib.add_log_level,
                  structlog.stdlib.PositionalArgumentsFormatter(),
                  structlog.processors.TimeStamper(fmt='iso'),
                  structlog.processors.StackInfoRenderer(),
                  structlog.processors.format_exc_info,
                  structlog.processors.JSONRenderer(indent=2, sort_keys=True)]
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
