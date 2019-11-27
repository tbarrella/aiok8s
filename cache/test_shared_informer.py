import asyncio
import time
import unittest

from kubernetes.client.models.v1_object_meta import V1ObjectMeta
from kubernetes.client.models.v1_pod import V1Pod

from . import clock, fake_controller_source, store
from .controller import (
    ResourceEventHandlerFuncs,
    deletion_handling_meta_namespace_key_func,
    new_informer,
)
from .shared_informer import new_shared_informer


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


class TestSharedInformer(unittest.TestCase):
    @async_test
    async def test_resync_check_period(self):
        source = fake_controller_source.FakeControllerSource()
        informer = new_shared_informer(source, V1Pod(), 12 * 3600)

        clock_ = clock.FakeClock(time.time())
        informer._clock = clock_
        informer._processor._clock = clock_

        listener1 = TestListener("listener1", 0)
        await informer.add_event_handler_with_resync_period(
            listener1, listener1._resync_period
        )
        self.assertEqual(informer._resync_check_period, 12 * 3600)
        self.assertEqual(informer._processor._listeners[0]._resync_period, 0)

        listener2 = TestListener("listener2", 60)
        await informer.add_event_handler_with_resync_period(
            listener2, listener2._resync_period
        )
        self.assertEqual(informer._resync_check_period, 60)
        self.assertEqual(informer._processor._listeners[0]._resync_period, 0)
        self.assertEqual(informer._processor._listeners[1]._resync_period, 60)

        listener3 = TestListener("listener3", 55)
        await informer.add_event_handler_with_resync_period(
            listener3, listener3._resync_period
        )
        self.assertEqual(informer._resync_check_period, 55)
        self.assertEqual(informer._processor._listeners[0]._resync_period, 0)
        self.assertEqual(informer._processor._listeners[1]._resync_period, 60)
        self.assertEqual(informer._processor._listeners[2]._resync_period, 55)

        listener4 = TestListener("listener4", 5)
        await informer.add_event_handler_with_resync_period(
            listener4, listener4._resync_period
        )
        self.assertEqual(informer._resync_check_period, 5)
        self.assertEqual(informer._processor._listeners[0]._resync_period, 0)
        self.assertEqual(informer._processor._listeners[1]._resync_period, 60)
        self.assertEqual(informer._processor._listeners[2]._resync_period, 55)
        self.assertEqual(informer._processor._listeners[3]._resync_period, 5)


class TestListener:
    def __init__(self, name, resync_period, *expected):
        self._resync_period = resync_period
        self._expected_item_names = set(expected)
        self._name = name

    async def on_add(self, obj):
        self._handle(obj)

    async def on_update(self, old, new):
        self._handle(new)

    async def on_delete(self, obj):
        pass

    def handle(self, obj):
        key, _ = store.meta_namespace_key_func(obj)
        object_meta = obj.metadata
        self._received_item_names.append(object_meta.name)


if __name__ == "__main__":
    unittest.main()
