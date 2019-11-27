import asyncio
import unittest

from kubernetes.client.models.v1_object_meta import V1ObjectMeta
from kubernetes.client.models.v1_pod import V1Pod

from .fake_controller_source import FakeControllerSource


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


class TestBrodcaster(unittest.TestCase):
    @async_test
    async def test_rc_number(self):
        def pod(name):
            return V1Pod(metadata=V1ObjectMeta(name=name))

        queue = asyncio.Queue(maxsize=3)
        for _ in range(3):
            await queue.put(None)

        source = FakeControllerSource()
        await source.add(pod("foo"))
        await source.modify(pod("foo"))
        await source.modify(pod("foo"))

        w = await source.watch(resource_version="1")
        asyncio.ensure_future(self.consume(w, ["2", "3"], queue))

        list_ = await source.list()
        self.assertEqual(list_.metadata.resource_version, "3")

        w2 = await source.watch(resource_version="2")
        asyncio.ensure_future(self.consume(w2, ["3"], queue))

        w3 = await source.watch(resource_version="3")
        asyncio.ensure_future(self.consume(w3, [], queue))
        await source.shutdown()
        await queue.join()

    async def consume(self, w, rvs, done):
        try:
            i = 0
            async for got in w:
                rv = rvs[i]
                got_rv = got["object"].metadata.resource_version
                self.assertEqual(got_rv, rv)
                i += 1
            self.assertEqual(i, len(rvs))
        finally:
            done.task_done()


if __name__ == "__main__":
    unittest.main()
