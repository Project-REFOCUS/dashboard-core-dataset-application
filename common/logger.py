import logging
import os


CRITICAL = logging.CRITICAL
ERROR = logging.ERROR
WARNING = logging.WARNING
INFO = logging.INFO
DEBUG = logging.DEBUG

logging_level_map = {
    'CRITICAL': CRITICAL,
    'ERROR': ERROR,
    'WARNING': WARNING,
    'INFO': INFO,
    'DEBUG': DEBUG
}


class Logger:

    def __init__(self, name):
        self.handler = logging.StreamHandler()
        self.handler.setFormatter(logging.Formatter('[%(levelname)s]:%(name)s - %(message)s'))
        self.logger = logging.getLogger(name)
        self.logger.addHandler(self.handler)
        self.logger.propagate = False
        self.name = name

        log_level = os.getenv(f'{name}.logging.level', os.getenv('logging.level'))
        if log_level and log_level in logging_level_map:
            self.set_level(logging_level_map[log_level])

    def set_level(self, level):
        if level in {INFO, DEBUG, WARNING, CRITICAL, ERROR}:
            self.handler.setLevel(level)
            self.logger.setLevel(level)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

