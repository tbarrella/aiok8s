import asyncio
import unittest

from kubernetes.client.models.v1_list_meta import V1ListMeta
from kubernetes.client.models.v1_object_meta import V1ObjectMeta
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.models.v1_pod_list import V1PodList
from kubernetes.client.models.v1_service import V1Service

from . import watch
from .fifo import FIFO
from .reflector import Reflector, StopRequestedError
from .store import meta_namespace_key_func, new_store
from .wait import FOREVER_TEST_TIMEOUT


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
        fw = watch.new_fake()

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
        fw = watch.new_fake()

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
        fw = watch.new_fake()
        fw.stop()
        with self.assertRaises(Exception):
            await g._watch_handler(fw, {}, asyncio.Queue(), asyncio.Event())

    @async_test
    async def test_reflector_watch_handler(self):
        s = new_store(meta_namespace_key_func)
        g = Reflector(TestLW(None, None), V1Pod(), s, 0)
        fw = watch.new_fake()
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

    @async_test
    async def test_reflector_stop_watch(self):
        s = new_store(meta_namespace_key_func)
        g = Reflector(TestLW(None, None), V1Pod(), s, 0)
        fw = watch.new_fake()
        stop_watch = asyncio.Event()
        stop_watch.set()
        with self.assertRaises(StopRequestedError):
            await g._watch_handler(fw, {}, asyncio.Queue(), stop_watch)

    @async_test
    async def test_reflector_list_and_watch(self):
        created_fakes = asyncio.Queue()
        expected_rvs = ["1", "3"]

        def list_func(**options):
            return V1PodList(metadata=V1ListMeta(resource_version="1"), items=[])

        def watch_func(**options):
            nonlocal expected_rvs
            rv = options["resource_version"]
            fw = watch.new_fake()
            self.assertEqual(rv, expected_rvs[0])
            expected_rvs = expected_rvs[1:]
            asyncio.ensure_future(created_fakes.put(fw))
            return fw

        lw = TestLW(list_func, watch_func)
        s = FIFO(meta_namespace_key_func)
        r = Reflector(lw, V1Pod(), s, 0)
        asyncio.ensure_future(r.list_and_watch(asyncio.Event()))

        ids = ["foo", "bar", "baz", "qux", "zoo"]
        fw = None
        for i, id_ in enumerate(ids):
            if not fw:
                fw = await created_fakes.get()
            sending_rv = str(i + 2)
            await fw.add(
                V1Pod(metadata=V1ObjectMeta(name=id_, resource_version=sending_rv))
            )
            if sending_rv == "3":
                fw.stop()
                fw = None

        for i, id_ in enumerate(ids):
            pod = await pop(s)
            self.assertEqual(pod.metadata.name, id_)
            self.assertEqual(pod.metadata.resource_version, str(i + 2))

        self.assertEqual(len(expected_rvs), 0)


class TestLW:
    def __init__(self, list_func, watch_func):
        self._list_func = list_func
        self._watch_func = watch_func

    def list(self, **options):
        return self._list_func(**options)

    def watch(self, **options):
        return self._watch_func(**options)


async def pop(queue_):
    result = None

    def process(obj):
        nonlocal result
        result = obj

    await queue_.pop(process)
    return result


if __name__ == "__main__":
    unittest.main()
