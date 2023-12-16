from .logger import Logger

import time


class PerformanceLogger:

    def __init__(self, name):
        self.logger = Logger(f'{name}.performance')
        self.internals = {}

    def start(self, name):
        self.internals[name] = round(time.perf_counter(), 3)
        return self.PerformanceTimer(name, self.stop)

    def stop(self, name):
        if name in self.internals:
            duration = round(time.perf_counter(), 3) - self.internals[name]
            self.logger.debug(f'{name} took {duration} seconds')

    class PerformanceTimer:

        def __init__(self, name, stop_timer):
            self.stop_timer = stop_timer
            self.name = name

        def stop(self):
            self.stop_timer(self.name)
