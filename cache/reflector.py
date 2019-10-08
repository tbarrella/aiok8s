import queue

from . import clock, wait


class Reflector:
    def __init__(self, lw, expected_type, store, resync_period):
        self.should_resync = None
        self.watch_list_page_size = 0
        # TODO: name
        self._store = store
        self._lister_watcher = lw
        self._period = 1
        self._resync_period = resync_period
        self._clock = clock.RealClock()
        self._set_expected_type(expected_type)

    def run(self, stop_event):
        def f():
            self.list_and_watch(stop_event)

        wait.until(f, self._period, stop_event)

    def _set_expected_type(self, expected_type):
        self._expected_type = type(expected_type)
        if expected_type is None:
            self._expected_type_name = _DEFAULT_EXPECTED_TYPE_NAME
            return
        self._expected_type_name = str(self._expected_type)
        # TODO: Handle Unstructured

    def _resync_queue(self):
        if not self._resync_period:
            return queue.Queue(), lambda: False
        t = self._clock.new_timer(self._resync_period)
        return t.c(), t.stop


_DEFAULT_EXPECTED_TYPE_NAME = "<unspecified>"
_MIN_WATCH_TIMEOUT = 5 * 60
