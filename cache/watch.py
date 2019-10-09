import queue
import threading


class EventType:
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"
    BOOKMARK = "BOOKMARK"
    ERROR = "ERROR"


class FakeWatcher:
    def __init__(self, result):
        self.stopped = False
        self._result = result
        self._mutex = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        if self.stopped:
            raise StopIteration
        event = self._result.get()
        self._result.task_done()
        if event is None:
            raise StopIteration
        return event

    def stop(self):
        with self._mutex:
            if not self.stopped:
                self._result.put(None)
                self.stopped = True

    def is_stopped(self):
        with self._mutex:
            return self.stopped

    def reset(self):
        with self._mutex:
            self.stopped = False
            self._result = queue.Queue()

    def add(self, obj):
        self.action(EventType.ADDED, obj)

    def modify(self, obj):
        self.action(EventType.MODIFIED, obj)

    def delete(self, obj):
        self.action(EventType.DELETED, obj)

    def error(self, obj):
        self.action(EventType.ERROR, obj)

    def action(self, action, obj):
        self._result.put({"type": action, "object": obj})
        self._result.join()


def new_fake():
    return FakeWatcher(queue.Queue())
