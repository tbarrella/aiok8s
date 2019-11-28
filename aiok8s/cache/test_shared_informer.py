# Copyright 2017 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import time
import unittest

from kubernetes.client.models import V1ObjectMeta, V1Pod

from aiok8s.cache.shared_informer import new_shared_informer
from aiok8s.cache.testing import fake_controller_source
from aiok8s.cache.testing.util import async_test
from aiok8s.util import clock, wait


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
