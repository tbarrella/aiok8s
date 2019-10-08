import queue
import threading
import time


class Timer:
    def __init__(self, d):
        self.c = queue.Queue(maxsize=1)

        def function():
            self.c.put(time.time())

        timer = threading.Timer(d, function)
        timer.daemon = True
        timer.start()
