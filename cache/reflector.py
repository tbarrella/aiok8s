import queue
import random
import threading

from . import clock, wait, watch


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
        self._last_sync_resource_version = ""
        # TODO: RWLock
        self._last_sync_resource_version_mutex = threading.Lock()
        self._set_expected_type(expected_type)

    def run(self, stop_event):
        def f():
            self.list_and_watch(stop_event)

        wait.until(f, self._period, stop_event)

    def list_and_watch(self, stop_event):
        options = {"resource_version": "0"}
        list_queue = queue.Queue(maxsize=1)
        cancel_event = threading.Event()

        class Stop(threading.Thread):
            daemon = True

            def run(self):
                stop_event.wait()
                list_queue.put(None)
                cancel_event.set()

        def list_target():
            try:
                list_ = self._lister_watcher.list(**options)
            except Exception as e:
                list_queue.put(e)
            else:
                list_queue.put(list_)

        Stop().start()
        t = threading.Thread(target=list_target)
        t.daemon = True
        t.start()
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

        resync_error_queue = queue.Queue(maxsize=1)

        class Stop(threading.Thread):
            daemon = True

            def run(self):
                stop_event.wait()
                select.put(StopRequestedError())

        def resync_target():
            resync_select = queue.Queue(maxsize=2)
            resync_queue, cleanup = self._resync_queue()

            def cancel():
                cancel_event.wait()
                resync_select.put(None)

            def forward(resync_queue):
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
                            resync_error_queue.put(e)
                            return
                    cleanup()
                    resync_queue, cleanup = self._resync_queue()
                    threading.Thread(target=forward, args=(resync_queue,)).start()
            finally:
                cleanup()

        t = threading.Thread(target=resync_target)
        t.daemon = True
        t.start()
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
                    self._watch_handler(w, options, resync_error_queue, stop_event)
                except Exception:
                    return
        finally:
            cancel_event.set()

    def last_sync_resource_version(self, v):
        with self._last_sync_resource_version_mutex:
            return self._last_sync_resource_version

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

    def _watch_handler(self, w, options, error_queue, stop_event):
        start = self._clock.now()
        event_count = 0
        select = queue.Queue(maxsize=3)

        class Stop(threading.Thread):
            daemon = True

            def run(self):
                stop_event.wait()
                select.put(StopRequestedError())

        class Error(threading.Thread):
            daemon = True

            def run(self):
                select.put(error_queue.get())

        class Forward(threading.Thread):
            daemon = True

            def run(self):
                for event in w:
                    select.put(event)
                select.put(None)

        Stop().start()
        Error().start()
        Forward().start()
        try:
            while True:
                event = select.get()
                if isinstance(event, Exception):
                    raise event
                if event is None:
                    break
                if event["type"] == watch.EventType.ERROR:
                    raise Exception
                if self._expected_type is not None and not isinstance(
                    event["object"], self._expected_type
                ):
                    continue
                # TODO: Handle GVK
                try:
                    meta = event["object"].metadata
                except AttributeError:
                    continue
                new_resource_version = meta.resource_version
                if event["type"] == watch.EventType.ADDED:
                    try:
                        self._store.add(event["object"])
                    except Exception:
                        pass
                elif event["type"] == watch.EventType.MODIFIED:
                    try:
                        self._store.update(event["object"])
                    except Exception:
                        pass
                elif event["type"] == watch.EventType.DELETED:
                    try:
                        self._store.delete(event["object"])
                    except Exception:
                        pass
                options["resource_version"] = new_resource_version
                self._set_last_sync_resource_version(new_resource_version)
                event_count += 1
            watch_duration = self._clock.since(start)
            if watch_duration < 1 and not event_count:
                raise Exception
        finally:
            w.stop()

    def _set_last_sync_resource_version(self, v):
        with self._last_sync_resource_version_mutex:
            self._last_sync_resource_version = v


_DEFAULT_EXPECTED_TYPE_NAME = "<unspecified>"
_MIN_WATCH_TIMEOUT = 5 * 60


class StopRequestedError(Exception):
    pass
