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
import random
import time
import unittest

from aiok8s.cache.testing.util import async_test
from aiok8s.util.wait import (
    Backoff,
    WaitTimeoutError,
    exponential_backoff,
    jitter_until,
    until,
)


class TestWait(unittest.TestCase):
    @async_test
    async def test_until(self):
        event = asyncio.Event()
        event.set()

        async def f():
            raise Exception("should not have been invoked")

        await until(f, 0, event)

        event = asyncio.Event()
        called = asyncio.Queue()

        async def f():
            await called.put(None)

        async def coro():
            await until(f, 0, event)
            await called.put(None)

        asyncio.ensure_future(coro())
        await called.get()
        event.set()
        await called.get()

    @async_test
    async def test_jitter_until(self):
        event = asyncio.Event()
        event.set()

        async def f():
            raise Exception("should not have been invoked")

        await jitter_until(f, 0, 1, True, event)

        event = asyncio.Event()
        called = asyncio.Queue()

        async def f():
            await called.put(None)

        async def coro():
            await jitter_until(f, 0, 1, True, event)
            await called.put(None)

        asyncio.ensure_future(coro())
        await called.get()
        event.set()
        await called.get()

    @async_test
    async def test_jitter_until_returns_immediately(self):
        now = time.time()
        event = asyncio.Event()

        async def f():
            event.set()

        await jitter_until(f, 30, 1, True, event)
        self.assertLessEqual(time.time(), now + 25)

    @async_test
    async def test_jitter_until_negative_factor(self):
        now = time.time()
        event = asyncio.Event()
        called = asyncio.Queue()
        received = asyncio.Queue()

        async def f():
            await called.put(None)
            await received.get()

        asyncio.ensure_future(jitter_until(f, 1, -30, True, event))
        await called.get()
        await received.put(None)
        await called.get()
        event.set()
        await received.put(None)
        self.assertLessEqual(time.time(), now + 3)

    @async_test
    async def test_exponential_backoff(self):
        opts = Backoff(factor=1, steps=3)

        i = 0

        async def condition():
            nonlocal i
            i += 1
            return False

        with self.assertRaises(WaitTimeoutError):
            await exponential_backoff(opts, condition)
        self.assertEqual(i, opts.steps)

        i = 0

        async def condition():
            nonlocal i
            i += 1
            return True

        await exponential_backoff(opts, condition)
        self.assertEqual(i, 1)

        class TestError(Exception):
            pass

        async def condition():
            raise TestError

        with self.assertRaises(TestError):
            await exponential_backoff(opts, condition)

        i = 1

        async def condition():
            nonlocal i
            if i < opts.steps:
                i += 1
                return False
            return True

        await exponential_backoff(opts, condition)
        self.assertEqual(i, opts.steps)

    def test_backoff_step(self):
        tests = [
            {"initial": Backoff(duration=1, steps=0), "want": [1, 1, 1]},
            {"initial": Backoff(duration=1, steps=1), "want": [1, 1, 1]},
            {"initial": Backoff(duration=1, factor=1, steps=1), "want": [1, 1, 1]},
            {"initial": Backoff(duration=1, factor=2, steps=3), "want": [1, 2, 4]},
            {
                "initial": Backoff(duration=1, factor=2, steps=3, cap=3),
                "want": [1, 2, 3],
            },
            {
                "initial": Backoff(duration=1, factor=2, steps=2, cap=3, jitter=0.5),
                "want": [2, 3, 3],
            },
            {
                "initial": Backoff(duration=1, factor=2, steps=6, jitter=4),
                "want": [1, 2, 4, 8, 16, 32],
            },
        ]
        for seed in range(5):
            for tt in tests:
                initial = Backoff(**tt["initial"].__dict__)
                random.seed(seed)
                for want in tt["want"]:
                    got = initial.step()
                    if initial.jitter:
                        self.assertNotEqual(got, want)
                        diff = (want - got) / want
                        self.assertLessEqual(diff, initial.jitter)
                    else:
                        self.assertEqual(got, want)


if __name__ == "__main__":
    unittest.main()
