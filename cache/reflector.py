import queue
import random
import threading

from . import clock, wait


class Reflector:
    def __init__(self, lw, expected_type, store, resync_period):
        self.should_resync = None
        # TODO: self.watch_list_page_size = 0
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

    def list_and_watch(self, stop_event):
        options = {"resource_version": "0"}
        list_queue = queue.Queue(maxsize=1)
        cancel_event = threading.Event()

        def stop():
            stop_event.wait()
            list_queue.put(None)
            cancel_event.set()

        threading.Thread(target=stop).start()

        def list_target():
            try:
                list_ = self._lister_watcher.list(**options)
            except Exception as e:
                list_queue.put(e)
            else:
                list_queue.put(list_)

        threading.Thread(target=list_target).start()
        r = list_queue.get()
        if stop_event.is_set():
            return
        if isinstance(r, Exception):
            raise r
        list_meta = r.metadata
        resource_version = list_meta.resource_version
        items = r.items
        self._sync_with(items, resource_version)
        self._set_last_sync_resource_version(resource_version)

        resync_exception_queue = queue.Queue(maxsize=1)

        def resync_target():
            resync_select = queue.Queue(maxsize=2)
            resync_queue, cleanup = self._resync_queue()

            def cancel():
                cancel_event.wait()
                resync_select.put(None)

            def forward(resync_queue):
                while True:
                    resync_select.put(resync_queue.get())

            threading.Thread(target=cancel).start()
            threading.Thread(target=forward, args=(resync_queue,)).start()
            try:
                while resync_select.get():
                    if cancel_event.is_set():
                        return
                    if self.should_resync is None or self.should_resync():
                        try:
                            self._store.resync()
                        except Exception as e:
                            resync_exception_queue.put(e)
                            return
                    cleanup()
                    resync_queue, cleanup = self._resync_queue()
                    threading.Thread(target=forward, args=(resync_queue,)).start()
            finally:
                cleanup()

        threading.Thread(target=resync_target).start()
        try:
            while not stop_event.is_set():
                timeout_seconds = _MIN_WATCH_TIMEOUT * (random.random() + 1)
                options = {
                    "resource_version": resource_version,
                    "timeout_seconds": timeout_seconds,
                    # TODO: AllowWatchBookmarks
                }
                try:
                    w = self._lister_watcher.watch(**options)
                except Exception:
                    # TODO: Handle ECONNREFUSED
                    return
                try:
                    self._watch_handler(w, options, resync_exception_queue, stop_event)
                except Exception:
                    return
        finally:
            cancel_event.set()

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

    def _sync_with(self, items, resource_version):
        found = list(items)
        self._store.replace(found, resource_version)

    def _watch_handler(self, w, options, exception_queue, stop_event):
        pass


_DEFAULT_EXPECTED_TYPE_NAME = "<unspecified>"
_MIN_WATCH_TIMEOUT = 5 * 60
