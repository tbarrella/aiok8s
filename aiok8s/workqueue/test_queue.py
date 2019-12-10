# Copyright 2015 The Kubernetes Authors.
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
import collections
import unittest

from aiok8s.cache.testing.util import async_test
from aiok8s.workqueue.queue import new


class TestQueue(unittest.TestCase):
    @async_test
    async def test_basic(self):
        q = new()

        async def produce(i):
            for j in range(50):
                await q.add(j)
                await asyncio.sleep(0.001)

        producers = 50
        producer_tasks = [asyncio.ensure_future(produce(i)) for i in range(producers)]

        async def consume(i):
            while True:
                item, quit = await q.get()
                if item == "added after shutdown!":
                    self.fail("Got an item added after shutdown.")
                if quit:
                    return
                await asyncio.sleep(0.003)
                await q.done(item)

        consumers = 10
        consumer_tasks = [asyncio.ensure_future(consume(i)) for i in range(consumers)]

        await asyncio.gather(*producer_tasks)
        await q.shut_down()
        await q.add("added after shutdown!")
        await asyncio.gather(*consumer_tasks)

    @async_test
    async def test_add_while_processing(self):
        q = new()

        producers = 50
        producer_tasks = [asyncio.ensure_future(q.add(i)) for i in range(producers)]

        async def consume(i):
            counters = collections.Counter()
            while True:
                item, quit = await q.get()
                if quit:
                    return
                counters[item] += 1
                if counters[item] < 2:
                    await q.add(item)
                await q.done(item)

        consumers = 10
        consumer_tasks = [asyncio.ensure_future(consume(i)) for i in range(consumers)]

        await asyncio.gather(*producer_tasks)
        await q.shut_down()
        await asyncio.gather(*consumer_tasks)

    @async_test
    async def test_len(self):
        q = new()
        await q.add("foo")
        self.assertEqual(len(q), 1)
        await q.add("bar")
        self.assertEqual(len(q), 2)
        await q.add("foo")
        self.assertEqual(len(q), 2)

    @async_test
    async def test_reinsert(self):
        q = new()
        await q.add("foo")
        i, _ = await q.get()
        self.assertEqual(i, "foo")

        await q.add(i)
        await q.done(i)

        i, _ = await q.get()
        self.assertEqual(i, "foo")

        await q.done(i)

        self.assertEqual(len(q), 0)


if __name__ == "__main__":
    unittest.main()
