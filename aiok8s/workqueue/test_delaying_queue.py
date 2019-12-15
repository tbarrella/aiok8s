# Copyright 2016 The Kubernetes Authors.
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

import time
import unittest

from aiok8s.cache.testing.util import async_test
from aiok8s.util import clock, wait
from aiok8s.workqueue.delaying_queue import _DelayingType


class TestDelayingQueue(unittest.TestCase):
    @async_test
    async def test_simple_queue(self):
        fake_clock = clock.FakeClock(time.time())
        q = _DelayingType(fake_clock)

        first = "foo"

        await q.add_after(first, 0.05)
        await wait_for_waiting_queue_to_fill(q)

        self.assertEqual(len(q), 0)

        await fake_clock.step(0.06)

        await wait_for_added(q, 1)
        item, _ = await q.get()
        await q.done(item)

        await fake_clock.step(10)

        async def condition():
            if len(q):
                self.fail("added to queue")
            return False

        with self.assertRaises(wait.WaitTimeoutError):
            await wait.poll(0.001, 0.03, condition)

        self.assertEqual(len(q), 0)

    @async_test
    async def test_deduping(self):
        fake_clock = clock.FakeClock(time.time())
        q = _DelayingType(fake_clock)

        first = "foo"

        await q.add_after(first, 0.05)
        await wait_for_waiting_queue_to_fill(q)
        await q.add_after(first, 0.07)
        await wait_for_waiting_queue_to_fill(q)
        self.assertEqual(len(q), 0)

        await fake_clock.step(0.06)
        await wait_for_added(q, 1)
        item, _ = await q.get()
        await q.done(item)

        await fake_clock.step(0.02)
        self.assertEqual(len(q), 0)

        await q.add_after(first, 0.05)
        await q.add_after(first, 0.03)
        await wait_for_waiting_queue_to_fill(q)
        self.assertEqual(len(q), 0)

        await fake_clock.step(0.04)
        await wait_for_added(q, 1)
        item, _ = await q.get()
        await q.done(item)

        await fake_clock.step(0.02)
        self.assertEqual(len(q), 0)

    @async_test
    async def test_add_two_fire_early(self):
        fake_clock = clock.FakeClock(time.time())
        q = _DelayingType(fake_clock)

        first = "foo"
        second = "bar"
        third = "baz"

        await q.add_after(first, 1)
        await q.add_after(second, 0.05)
        await wait_for_waiting_queue_to_fill(q)

        self.assertEqual(len(q), 0)

        await fake_clock.step(0.06)
        await wait_for_added(q, 1)
        item, _ = await q.get()
        self.assertEqual(item, second)

        await q.add_after(third, 2)

        await fake_clock.step(1)
        await wait_for_added(q, 1)
        item, _ = await q.get()
        self.assertEqual(item, first)

        await fake_clock.step(2)
        await wait_for_added(q, 1)
        item, _ = await q.get()
        self.assertEqual(item, third)

    @async_test
    async def test_copy_shifting(self):
        fake_clock = clock.FakeClock(time.time())
        q = _DelayingType(fake_clock)

        first = "foo"
        second = "bar"
        third = "baz"

        await q.add_after(first, 1)
        await q.add_after(second, 0.5)
        await q.add_after(third, 0.25)
        await wait_for_waiting_queue_to_fill(q)

        self.assertEqual(len(q), 0)

        await fake_clock.step(2)
        await wait_for_added(q, 3)
        actual_first, _ = await q.get()
        self.assertEqual(actual_first, third)
        actual_second, _ = await q.get()
        self.assertEqual(actual_second, second)
        actual_third, _ = await q.get()
        self.assertEqual(actual_third, first)


async def wait_for_added(q, depth):
    async def condition():
        return len(q) == depth

    return await wait.poll(0.001, 10, condition)


async def wait_for_waiting_queue_to_fill(q):
    async def condition():
        return q._waiting_for_add_queue.empty()

    return await wait.poll(0.001, 10, condition)


if __name__ == "__main__":
    unittest.main()
