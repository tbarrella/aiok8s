import queue
import threading
import time
import unittest

from .wait import jitter_until, until


class TestWait(unittest.TestCase):
    def test_jitter(self):
        event = threading.Event()
        event.set()

        def f():
            raise Exception("should not have been invoked")

        until(f, 0, event)

        event = threading.Event()
        called = queue.Queue()

        def f():
            called.put(None)

        def target():
            until(f, 0, event)
            called.put(None)

        threading.Thread(target=target).start()
        called.get()
        event.set()
        called.get()

    def test_jitter_until(self):
        event = threading.Event()
        event.set()

        def f():
            raise Exception("should not have been invoked")

        jitter_until(f, 0, 1, True, event)

        event = threading.Event()
        called = queue.Queue()

        def f():
            called.put(None)

        def target():
            jitter_until(f, 0, 1, True, event)
            called.put(None)

        threading.Thread(target=target).start()
        called.get()
        event.set()
        called.get()

    def test_jitter_until_returns_immediately(self):
        now = time.time()
        event = threading.Event()
        jitter_until(event.set, 30, 1, True, event)
        self.assertLessEqual(time.time(), now + 25)

    def test_jitter_until_negative_factor(self):
        now = time.time()
        event = threading.Event()
        called = queue.Queue()
        received = queue.Queue()

        def f():
            called.put(None)
            received.get()

        threading.Thread(target=jitter_until, args=(f, 1, -30, True, event)).start()
        called.get()
        received.put(None)
        called.get()
        event.set()
        received.put(None)
        self.assertLessEqual(time.time(), now + 3)


if __name__ == "__main__":
    unittest.main()
