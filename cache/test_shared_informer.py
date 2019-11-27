import asyncio
import time
import unittest

from kubernetes.client.models.v1_object_meta import V1ObjectMeta
from kubernetes.client.models.v1_pod import V1Pod

from . import clock, fake_controller_source, store, wait
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
    async def test_listener_resync_periods(self):
        source = fake_controller_source.FakeControllerSource()
        await source.add(V1Pod(metadata=V1ObjectMeta(name="pod1")))
        await source.add(V1Pod(metadata=V1ObjectMeta(name="pod2")))

        informer = new_shared_informer(source, V1Pod(), 1)

        clock_ = clock.FakeClock(time.time())
        informer._clock = clock_
        informer._processor._clock = clock_

        listener1 = TestListener("listener1", 0, "pod1", "pod2")
        await informer.add_event_handler_with_resync_period(
            listener1, listener1._resync_period
        )

        listener2 = TestListener("listener2", 2, "pod1", "pod2")
        await informer.add_event_handler_with_resync_period(
            listener2, listener2._resync_period
        )

        listener3 = TestListener("listener3", 3, "pod1", "pod2")
        await informer.add_event_handler_with_resync_period(
            listener3, listener3._resync_period
        )
        listeners = [listener1, listener2, listener3]

        stop = asyncio.Event()
        try:
            asyncio.ensure_future(informer.run(stop))

            for listener in listeners:
                self.assertTrue(await listener._ok())

            for listener in listeners:
                listener._received_item_names = []

            await clock_.step(2)
            self.assertTrue(await listener2._ok())

            await asyncio.sleep(1)
            self.assertEqual(len(listener1._received_item_names), 0)
            self.assertEqual(len(listener3._received_item_names), 0)

            for listener in listeners:
                listener._received_item_names = []

            await clock_.step(1)
            self.assertTrue(await listener3._ok())

            await asyncio.sleep(1)
            self.assertEqual(len(listener1._received_item_names), 0)
            self.assertEqual(len(listener2._received_item_names), 0)
        finally:
            stop.set()

            # TODO: Figure out why this is necessary...
            await asyncio.sleep(0.1)

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

    @async_test
    async def test_initialization_race(self):
        source = fake_controller_source.FakeControllerSource()
        informer = new_shared_informer(source, V1Pod(), 1)
        listener = TestListener("race_listener", 0)

        stop = asyncio.Event()
        asyncio.ensure_future(
            informer.add_event_handler_with_resync_period(
                listener, listener._resync_period
            )
        )
        asyncio.ensure_future(informer.run(stop))
        stop.set()


class TestListener:
    def __init__(self, name, resync_period, *expected):
        self._resync_period = resync_period
        self._expected_item_names = set(expected)
        self._received_item_names = []
        self._name = name

    async def on_add(self, obj):
        self._handle(obj)

    async def on_update(self, old, new):
        self._handle(new)

    async def on_delete(self, obj):
        pass

    def _handle(self, obj):
        object_meta = obj.metadata
        self._received_item_names.append(object_meta.name)

    async def _ok(self):
        try:
            await wait.poll_immediate(0.1, 2, self._satisfied_expectations)
        except Exception:
            return False

        await asyncio.sleep(1)
        return self._satisfied_expectations()

    def _satisfied_expectations(self):
        return (
            len(self._received_item_names) == len(self._expected_item_names)
            and set(self._received_item_names) == self._expected_item_names
        )


if __name__ == "__main__":
    unittest.main()
