# Copyright 2014 The Kubernetes Authors.
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
import unittest
from typing import NamedTuple, Optional

from aiok8s.cache.testing.util import async_test
from aiok8s.util import wait
from aiok8s.watch import watch
from aiok8s.watch.mux import Broadcaster, FullChannelBehavior


class TestBrodcaster(unittest.TestCase):
    @async_test
    async def test(self):
        table = [
            {"type": watch.EventType.ADDED, "object": MyType("foo", "hello world 1")},
            {"type": watch.EventType.ADDED, "object": MyType("bar", "hello world 2")},
            {
                "type": watch.EventType.MODIFIED,
                "object": MyType("foo", "goodbye world 3"),
            },
            {"type": watch.EventType.DELETED, "object": MyType("bar", "hello world 4")},
        ]

        m = Broadcaster(0, FullChannelBehavior.WAIT_IF_CHANNEL_FULL)
        test_watchers = 2

        async def coro(watcher, w):
            table_line = 0
            async for event in w:
                self.assertEqual(
                    event,
                    table[table_line],
                    msg="Watcher {}, line {}".format(watcher, table_line),
                )
                table_line += 1

        tasks = [
            asyncio.ensure_future(coro(i, await m.watch()))
            for i in range(test_watchers)
        ]
        for item in table:
            await m.action(item["type"], item["object"])
        await m.shutdown()
        await asyncio.gather(*tasks)

    @async_test
    async def test_watcher_close(self):
        m = Broadcaster(0, FullChannelBehavior.WAIT_IF_CHANNEL_FULL)
        w = await m.watch()
        w2 = await m.watch()
        await w.stop()
        await m.shutdown()
        async for _ in w:
            self.fail("Stop didn't work?")
        async for _ in w2:
            self.fail("Shutdown didn't work?")
        await w.stop()
        await w2.stop()

    @async_test
    async def test_watcher_stop_deadlock(self):
        done = asyncio.Event()
        m = Broadcaster(0, FullChannelBehavior.WAIT_IF_CHANNEL_FULL)

        async def coro(w0, w1):
            async def aw0():
                async for _ in w0:
                    await w1.stop()

            async def aw1():
                async for _ in w1:
                    await w0.stop()

            await asyncio.wait(
                [asyncio.ensure_future(aw0()), asyncio.ensure_future(aw1())],
                return_when=asyncio.FIRST_COMPLETED,
            )
            done.set()

        asyncio.ensure_future(coro(await m.watch(), await m.watch()))
        await m.action(watch.EventType.ADDED, MyType())
        await asyncio.wait_for(done.wait(), wait.FOREVER_TEST_TIMEOUT)
        await m.shutdown()

    @async_test
    async def test_drop_if_channel_full(self):
        m = Broadcaster(1, FullChannelBehavior.DROP_IF_CHANNEL_FULL)
        event1 = {
            "type": watch.EventType.ADDED,
            "object": MyType("foo", "hello world 1"),
        }
        event2 = {
            "type": watch.EventType.ADDED,
            "object": MyType("bar", "hello world 2"),
        }

        watches = [await m.watch() for _ in range(2)]

        await m.action(event1["type"], event1["object"])
        await m.action(event2["type"], event2["object"])
        await m.shutdown()

        async def coro(watcher, w):
            e1 = await w.__anext__()
            self.assertEqual(e1, event1, msg="Watcher {}".format(watcher))
            async for e2 in w:
                self.fail(
                    "Watcher {} received second event {!r} "
                    "even though it shouldn't have.".format(watcher, e2)
                )

        tasks = [asyncio.ensure_future(coro(i, w)) for i, w in enumerate(watches)]
        await asyncio.gather(*tasks)


class MyType(NamedTuple):
    id: Optional[str] = None
    value: Optional[str] = None


if __name__ == "__main__":
    unittest.main()
