import queue
import threading
import time


class Timer:
    def __init__(self, d):
        self.c = queue.Queue(maxsize=1)

        def function():
            self.c.put(time.time())

        self._timer = threading.Timer(d, function)
        self._timer.daemon = True
        self._timer.start()

    def stop(self):
        self._timer.cancel()
