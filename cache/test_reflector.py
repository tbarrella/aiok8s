import asyncio
import unittest

from kubernetes.client.models.v1_list_meta import V1ListMeta
from kubernetes.client.models.v1_object_meta import V1ObjectMeta
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.models.v1_pod_list import V1PodList

from .reflector import Reflector
from .store import meta_namespace_key_func, new_store
from .wait import FOREVER_TEST_TIMEOUT
from .watch import new_fake


def run(main, *, debug=False):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        loop.set_debug(debug)
        return loop.run_until_complete(main)
    finally:
        try:
            _cancel_all_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def _cancel_all_tasks(loop):
    to_cancel = asyncio.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    loop.run_until_complete(
        asyncio.gather(*to_cancel, loop=loop, return_exceptions=True)
    )

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during asyncio.run() shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )


def async_test(coro):
    def wrapper(*args, **kwargs):
        return run(coro(*args, **kwargs))

    return wrapper


class TestReflector(unittest.TestCase):
    @async_test
    async def test_close_watch_on_error(self):
        pod = V1Pod(metadata=V1ObjectMeta(name="bar"))
        fw = new_fake()

        def list_func(**options):
            return V1PodList(metadata=V1ListMeta(resource_version="1"), items=[])

        lister_watcher = TestLW(list_func, lambda **_: fw)
        r = Reflector(lister_watcher, V1Pod(), new_store(meta_namespace_key_func), 0)
        asyncio.ensure_future(r.list_and_watch(asyncio.Event()))
        await fw.error(pod)

        async def target():
            async for _ in fw:
                assert False

        await asyncio.wait_for(target(), FOREVER_TEST_TIMEOUT)

    @async_test
    async def test_run_until(self):
        stop_event = asyncio.Event()
        store = new_store(meta_namespace_key_func)
        fw = new_fake()

        def list_func(**options):
            return V1PodList(metadata=V1ListMeta(resource_version="1"), items=[])

        lister_watcher = TestLW(list_func, lambda **_: fw)
        r = Reflector(lister_watcher, V1Pod(), store, 0)
        asyncio.ensure_future(r.run(stop_event))
        await fw.add(V1Pod(metadata=V1ObjectMeta(name="bar")))
        stop_event.set()

        async def target():
            async for _ in fw:
                assert False

        await asyncio.wait_for(target(), FOREVER_TEST_TIMEOUT)


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
