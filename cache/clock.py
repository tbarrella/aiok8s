import time

from . import time_


class RealClock:
    def now(self):
        return time.time()

    def since(self, ts):
        return time.time() - ts

    def new_timer(self, d):
        return _RealTimer(time_.Timer(d))


class _RealTimer:
    def __init__(self, timer):
        self._timer = timer

    def c(self):
        return self._timer.c

    def stop(self):
        return self._timer.stop()
