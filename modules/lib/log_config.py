#!/usr/bin/env python3
import structlog
import logging
import sys


def configure(log_level):
    """
    Configure log for stdout JSON.

    :param log_level: The log level to set.
    :type log_level: str
    :return:
    """
    level = get_level(log_level)
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    root_logger.addHandler(handler)

    processors = [structlog.stdlib.filter_by_level, structlog.stdlib.add_logger_name, structlog.stdlib.add_log_level,
                  structlog.stdlib.PositionalArgumentsFormatter(), structlog.processors.TimeStamper(fmt='iso'),
                  structlog.processors.StackInfoRenderer(), structlog.processors.format_exc_info,
                  structlog.processors.JSONRenderer(indent=2, sort_keys=True)]

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_level(level_name):
    """
    Get log level by name.

    :param level_name: The level name.
    :type level_name: str
    :return: The level.
    """
    log_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARN': logging.WARN,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    return log_levels[level_name]
