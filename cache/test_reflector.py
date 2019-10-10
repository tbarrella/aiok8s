import threading
import unittest

from kubernetes.client.models.v1_list_meta import V1ListMeta
from kubernetes.client.models.v1_object_meta import V1ObjectMeta
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.models.v1_pod_list import V1PodList

from .reflector import Reflector
from .store import meta_namespace_key_func, new_store
from .wait import FOREVER_TEST_TIMEOUT, NeverStop
from .watch import new_fake


class TestReflector(unittest.TestCase):
    def test_close_watch_on_error(self):
        pod = V1Pod(metadata=V1ObjectMeta(name="bar"))
        fw = new_fake()

        def list_func(**options):
            return V1PodList(metadata=V1ListMeta(resource_version="1"), items=[])

        lister_watcher = TestLW(list_func, lambda **_: fw)
        r = Reflector(lister_watcher, V1Pod(), new_store(meta_namespace_key_func), 0)
        threading.Thread(target=r.list_and_watch, args=(NeverStop,)).start()
        fw.error(pod)
        timeout_event = threading.Event()

        def target():
            with self.assertRaises(StopIteration):
                next(fw)
            timeout_event.set()

        threading.Thread(target=target).start()
        timeout_event.wait(timeout=FOREVER_TEST_TIMEOUT)

    def test_run_until(self):
        stop_event = threading.Event()
        store = new_store(meta_namespace_key_func)
        fw = new_fake()

        def list_func(**options):
            return V1PodList(metadata=V1ListMeta(resource_version="1"), items=[])

        lister_watcher = TestLW(list_func, lambda **_: fw)
        r = Reflector(lister_watcher, V1Pod(), store, 0)
        threading.Thread(target=r.run, args=(stop_event,)).start()
        fw.add(V1Pod(metadata=V1ObjectMeta(name="bar")))
        stop_event.set()
        timeout_event = threading.Event()

        def target():
            with self.assertRaises(StopIteration):
                next(fw)
            timeout_event.set()

        threading.Thread(target=target).start()
        timeout_event.wait(timeout=FOREVER_TEST_TIMEOUT)


class TestLW:
    def __init__(self, list_func, watch_func):
        self._list_func = list_func
        self._watch_func = watch_func

    def list(self, **options):
        return self._list_func(**options)

    def watch(self, **options):
        return self._watch_func(**options)


if __name__ == "__main__":
    unittest.main()
