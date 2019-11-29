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

from aiok8s.cache.testing.util import async_test
from aiok8s.watch.watch import EventType, new_fake


class TestWatch(unittest.TestCase):
    @async_test
    async def test_fake(self):
        f = new_fake()

        table = [
            {"t": EventType.ADDED, "s": TestType("foo")},
            {"t": EventType.MODIFIED, "s": TestType("qux")},
            {"t": EventType.MODIFIED, "s": TestType("bar")},
            {"t": EventType.DELETED, "s": TestType("bar")},
            {"t": EventType.ERROR, "s": TestType("error: blah")},
        ]

        async def consumer(w):
            i = 0
            async for got in w:
                expect = table[i]
                self.assertEqual(got["type"], expect["t"])
                self.assertEqual(got["object"], expect["s"])
                i += 1
            self.assertEqual(i, len(table))

        async def sender():
            await f.add(TestType("foo"))
            await f.action(EventType.MODIFIED, TestType("qux"))
            await f.modify(TestType("bar"))
            await f.delete(TestType("bar"))
            await f.error(TestType("error: blah"))
            await f.stop()

        async with f:
            asyncio.ensure_future(sender())
            await consumer(f)


class TestType(str):
    pass


if __name__ == "__main__":
    unittest.main()
