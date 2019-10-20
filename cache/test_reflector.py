import asyncio
import unittest

from kubernetes.client.models.v1_list_meta import V1ListMeta
from kubernetes.client.models.v1_object_meta import V1ObjectMeta
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.models.v1_pod_list import V1PodList
from kubernetes.client.models.v1_service import V1Service

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

        async def aw():
            async for _ in fw:
                assert False

        await asyncio.wait_for(aw(), FOREVER_TEST_TIMEOUT)

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

        async def aw():
            async for _ in fw:
                assert False

        await asyncio.wait_for(aw(), FOREVER_TEST_TIMEOUT)

    @async_test
    async def test_reflector_resync_queue(self):
        s = new_store(meta_namespace_key_func)
        g = Reflector(TestLW(None, None), V1Pod(), s, 0.001)
        a, _ = g._resync_queue()
        await asyncio.wait_for(a.get(), FOREVER_TEST_TIMEOUT)

    @async_test
    async def test_reflector_watch_handler_error(self):
        s = new_store(meta_namespace_key_func)
        g = Reflector(TestLW(None, None), V1Pod(), s, 0)
        fw = new_fake()
        fw.stop()
        with self.assertRaises(Exception):
            await g._watch_handler(fw, {}, asyncio.Queue(), asyncio.Event())

    @async_test
    async def test_reflector_watch_handler(self):
        s = new_store(meta_namespace_key_func)
        g = Reflector(TestLW(None, None), V1Pod(), s, 0)
        fw = new_fake()
        await s.add(V1Pod(metadata=V1ObjectMeta(name="foo")))
        await s.add(V1Pod(metadata=V1ObjectMeta(name="bar")))

        async def aw():
            await fw.add(V1Service(metadata=V1ObjectMeta(name="rejected")))
            await fw.delete(V1Pod(metadata=V1ObjectMeta(name="foo")))
            await fw.modify(
                V1Pod(metadata=V1ObjectMeta(name="bar", resource_version="55"))
            )
            await fw.add(
                V1Pod(metadata=V1ObjectMeta(name="baz", resource_version="32"))
            )
            fw.stop()

        asyncio.ensure_future(aw())
        options = {}
        await g._watch_handler(fw, options, asyncio.Queue(), asyncio.Event())

        def mk_pod(id_, rv):
            return V1Pod(metadata=V1ObjectMeta(name=id_, resource_version=rv))

        table = [
            {"pod": mk_pod("foo", ""), "exists": False},
            {"pod": mk_pod("rejected", ""), "exists": False},
            {"pod": mk_pod("bar", "55"), "exists": True},
            {"pod": mk_pod("baz", "32"), "exists": True},
        ]
        for item in table:
            obj = s.get(item["pod"])
            exists = obj is not None
            self.assertIs(exists, item["exists"])
            if not exists:
                continue
            self.assertEqual(
                obj.metadata.resource_version, item["pod"].metadata.resource_version
            )

        self.assertEqual(options["resource_version"], "32")
        self.assertEqual(g.last_sync_resource_version(), "32")


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
