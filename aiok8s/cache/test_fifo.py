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
from typing import Any, NamedTuple

from aiok8s.cache.fifo import FIFO, ProcessError, RequeueError
from aiok8s.cache.testing.util import async_test


class TestFIFO(unittest.TestCase):
    @async_test
    async def test_basic(self):
        f = FIFO(test_fifo_object_key_func)
        amount = 500

        async def add_ints():
            for i in range(amount):
                await f.add(mk_fifo_obj(f"a{i}", i + 1))

        async def add_floats():
            for i in range(amount):
                await f.add(mk_fifo_obj(f"b{i}", i + 1.0))

        last_int = 0
        last_float = 0.0
        asyncio.ensure_future(add_ints())
        asyncio.ensure_future(add_floats())
        for _ in range(amount * 2):
            obj = (await pop(f)).val
            if isinstance(obj, int):
                self.assertGreater(obj, last_int)
                last_int = obj
            elif isinstance(obj, float):
                self.assertGreater(obj, last_float)
                last_float = obj
            else:
                self.fail("unexpected type {!r}".format(obj))

    @async_test
    async def test_requeue_on_pop(self):
        f = FIFO(test_fifo_object_key_func)
        await f.add(mk_fifo_obj("foo", 10))

        def process(obj):
            self.assertEqual(obj.name, "foo")
            raise RequeueError

        await f.pop(process)
        self.assertIsNotNone(f.get_by_key("foo"))

        class TestError(Exception):
            pass

        def process(obj):
            self.assertEqual(obj.name, "foo")
            raise RequeueError from TestError

        with self.assertRaises(ProcessError) as cm:
            await f.pop(process)
        self.assertIsInstance(cm.exception.__cause__, TestError)
        self.assertIsNotNone(f.get_by_key("foo"))

        def process(obj):
            self.assertEqual(obj.name, "foo")

        await f.pop(process)
        self.assertIsNone(f.get_by_key("foo"))

    @async_test
    async def test_add_update(self):
        f = FIFO(test_fifo_object_key_func)
        await f.add(mk_fifo_obj("foo", 10))
        await f.update(mk_fifo_obj("foo", 15))

        self.assertEqual(f.list(), [mk_fifo_obj("foo", 15)])
        self.assertEqual(f.list_keys(), ["foo"])

        got = asyncio.Queue(maxsize=2)

        async def get_popped():
            while True:
                await got.put(await pop(f))

        asyncio.ensure_future(get_popped())
        first = await got.get()
        self.assertEqual(first.val, 15)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(got.get(), 0.05)
        self.assertIsNone(f.get(mk_fifo_obj("foo", "")))

    @async_test
    async def test_add_replace(self):
        f = FIFO(test_fifo_object_key_func)
        await f.add(mk_fifo_obj("foo", 10))
        await f.replace([mk_fifo_obj("foo", 15)], "15")
        got = asyncio.Queue(maxsize=2)

        async def get_popped():
            while True:
                await got.put(await pop(f))

        asyncio.ensure_future(get_popped())
        first = await got.get()
        self.assertEqual(first.val, 15)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(got.get(), 0.05)
        self.assertIsNone(f.get(mk_fifo_obj("foo", "")))

    @async_test
    async def test_detect_line_jumpers(self):
        f = FIFO(test_fifo_object_key_func)

        await f.add(mk_fifo_obj("foo", 10))
        await f.add(mk_fifo_obj("bar", 1))
        await f.add(mk_fifo_obj("foo", 11))
        await f.add(mk_fifo_obj("foo", 13))
        await f.add(mk_fifo_obj("zab", 30))

        self.assertEqual((await pop(f)).val, 13)

        await f.add(mk_fifo_obj("foo", 14))
        self.assertEqual((await pop(f)).val, 1)
        self.assertEqual((await pop(f)).val, 30)
        self.assertEqual((await pop(f)).val, 14)

    @async_test
    async def test_add_if_not_present(self):
        f = FIFO(test_fifo_object_key_func)

        await f.add(mk_fifo_obj("a", 1))
        await f.add(mk_fifo_obj("b", 2))
        await f.add_if_not_present(mk_fifo_obj("b", 3))
        await f.add_if_not_present(mk_fifo_obj("c", 4))

        self.assertEqual(len(f._items), 3)

        expected_values = [1, 2, 4]
        for expected in expected_values:
            self.assertEqual((await pop(f)).val, expected)

    @async_test
    async def test_has_synced(self):
        tests = [
            {"actions": [], "expected_synced": False},
            {
                "actions": [lambda f: f.add(mk_fifo_obj("a", 1))],
                "expected_synced": True,
            },
            {"actions": [lambda f: f.replace([], "0")], "expected_synced": True},
            {
                "actions": [
                    lambda f: f.replace([mk_fifo_obj("a", 1), mk_fifo_obj("b", 2)], "0")
                ],
                "expected_synced": False,
            },
            {
                "actions": [
                    lambda f: f.replace(
                        [mk_fifo_obj("a", 1), mk_fifo_obj("b", 2)], "0"
                    ),
                    pop,
                ],
                "expected_synced": False,
            },
            {
                "actions": [
                    lambda f: f.replace(
                        [mk_fifo_obj("a", 1), mk_fifo_obj("b", 2)], "0"
                    ),
                    pop,
                    pop,
                ],
                "expected_synced": True,
            },
        ]
        for test in tests:
            f = FIFO(test_fifo_object_key_func)
            for action in test["actions"]:
                await action(f)
            self.assertEqual(f.has_synced(), test["expected_synced"])


async def pop(queue_):
    result = None

    def process(obj):
        nonlocal result
        result = obj

    await queue_.pop(process)
    return result


def test_fifo_object_key_func(obj):
    return obj.name


class TestFifoObject(NamedTuple):
    name: str
    val: Any


def mk_fifo_obj(name, val):
    return TestFifoObject(name, val)


if __name__ == "__main__":
    unittest.main()
