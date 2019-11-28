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

from aiok8s.cache import fifo
from aiok8s.cache.delta_fifo import (
    DeletedFinalStateUnknown,
    Delta,
    DeltaFIFO,
    Deltas,
    DeltaType,
)
from aiok8s.cache.testing.util import async_test


class TestDeltaFIFO(unittest.TestCase):
    @async_test
    async def test_basic(self):
        f = DeltaFIFO(test_fifo_object_key_func)
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
        for i in range(amount * 2):
            obj = (await test_pop(f)).val
            if isinstance(obj, int):
                self.assertGreater(obj, last_int)
                last_int = obj
            elif isinstance(obj, float):
                self.assertGreater(obj, last_float)
                last_float = obj
            else:
                assert False, f"unexpected type {obj!r}"

    @async_test
    async def test_requeue_on_pop(self):
        f = DeltaFIFO(test_fifo_object_key_func)
        await f.add(mk_fifo_obj("foo", 10))

        async def process(obj):
            self.assertEqual(obj[0].object.name, "foo")
            raise fifo.RequeueError

        await f.pop(process)
        self.assertIsNotNone(f.get_by_key("foo"))

        class TestError(Exception):
            pass

        async def process(obj):
            self.assertEqual(obj[0].object.name, "foo")
            raise fifo.RequeueError from TestError

        try:
            await f.pop(process)
        except fifo.ProcessError as e:
            self.assertIsInstance(e.__cause__, TestError)
        else:
            assert False, "expected error"
        self.assertIsNotNone(f.get_by_key("foo"))

        async def process(obj):
            self.assertEqual(obj[0].object.name, "foo")

        await f.pop(process)
        self.assertIsNone(f.get_by_key("foo"))

    @async_test
    async def test_add_update(self):
        f = DeltaFIFO(test_fifo_object_key_func)
        await f.add(mk_fifo_obj("foo", 10))
        await f.update(mk_fifo_obj("foo", 12))
        await f.delete(mk_fifo_obj("foo", 15))

        self.assertEqual(f.list(), [mk_fifo_obj("foo", 15)])
        self.assertEqual(f.list_keys(), ["foo"])

        got = asyncio.Queue(maxsize=2)

        async def get_popped():
            while True:
                obj = await test_pop(f)
                await got.put(obj)

        asyncio.ensure_future(get_popped())
        first = await got.get()
        self.assertEqual(first.val, 15)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(got.get(), 0.05)
        self.assertIsNone(f.get(mk_fifo_obj("foo", "")))

    @async_test
    async def test_enqueueing_no_lister(self):
        f = DeltaFIFO(test_fifo_object_key_func)
        await f.add(mk_fifo_obj("foo", 10))
        await f.update(mk_fifo_obj("bar", 15))
        await f.add(mk_fifo_obj("qux", 17))
        await f.delete(mk_fifo_obj("qux", 18))

        await f.delete(mk_fifo_obj("baz", 20))

        expect_list = [10, 15, 18]
        for expect in expect_list:
            self.assertEqual((await test_pop(f)).val, expect)
        self.assertEqual(len(f._items), 0)

    @async_test
    async def test_enqueueing_with_lister(self):
        f = DeltaFIFO(
            test_fifo_object_key_func,
            known_objects=KeyLookupFunc(
                lambda: [
                    mk_fifo_obj("foo", 5),
                    mk_fifo_obj("bar", 6),
                    mk_fifo_obj("baz", 7),
                ]
            ),
        )
        await f.add(mk_fifo_obj("foo", 10))
        await f.update(mk_fifo_obj("bar", 15))

        await f.delete(mk_fifo_obj("baz", 20))

        expect_list = [10, 15, 20]
        for expect in expect_list:
            self.assertEqual((await test_pop(f)).val, expect)
        self.assertEqual(len(f._items), 0)

    @async_test
    async def test_add_replace(self):
        f = DeltaFIFO(test_fifo_object_key_func)
        await f.add(mk_fifo_obj("foo", 10))
        await f.replace([mk_fifo_obj("foo", 15)], "0")
        got = asyncio.Queue(maxsize=2)

        async def get_popped():
            while True:
                await got.put(await test_pop(f))

        asyncio.ensure_future(get_popped())
        first = await got.get()
        self.assertEqual(first.val, 15)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(got.get(), 0.05)
        self.assertIsNone(f.get(mk_fifo_obj("foo", "")))

    @async_test
    async def test_resync_non_existing(self):
        f = DeltaFIFO(
            test_fifo_object_key_func,
            known_objects=KeyLookupFunc(lambda: [mk_fifo_obj("foo", 5)]),
        )
        await f.delete(mk_fifo_obj("foo", 10))
        await f.resync()

        deltas = f._items["foo"]
        self.assertEqual(len(deltas), 1)
        self.assertIs(deltas[0].type, DeltaType.DELETED)

    @async_test
    async def test_delete_existing_non_propagated(self):
        f = DeltaFIFO(
            test_fifo_object_key_func, known_objects=KeyLookupFunc(lambda: [])
        )
        await f.add(mk_fifo_obj("foo", 5))
        await f.delete(mk_fifo_obj("foo", 6))

        deltas = f._items["foo"]
        self.assertEqual(len(deltas), 2)
        self.assertIs(deltas[-1].type, DeltaType.DELETED)

    @async_test
    async def test_replace_makes_deletions(self):
        f = DeltaFIFO(
            test_fifo_object_key_func,
            known_objects=KeyLookupFunc(
                lambda: [
                    mk_fifo_obj("foo", 5),
                    mk_fifo_obj("bar", 6),
                    mk_fifo_obj("baz", 7),
                ]
            ),
        )
        await f.delete(mk_fifo_obj("baz", 10))
        await f.replace([mk_fifo_obj("foo", 5)], "0")

        expected_list = [
            Deltas([Delta(DeltaType.DELETED, mk_fifo_obj("baz", 10))]),
            Deltas([Delta(DeltaType.SYNC, mk_fifo_obj("foo", 5))]),
            Deltas(
                [
                    Delta(
                        DeltaType.DELETED,
                        DeletedFinalStateUnknown("bar", mk_fifo_obj("bar", 6)),
                    )
                ]
            ),
        ]
        for expected in expected_list:
            cur = await pop(f)
            self.assertEqual(cur, expected)

    @async_test
    async def test_update_resync_race(self):
        f = DeltaFIFO(
            test_fifo_object_key_func,
            known_objects=KeyLookupFunc(lambda: [mk_fifo_obj("foo", 5)]),
        )
        await f.update(mk_fifo_obj("foo", 6))
        await f.resync()

        expected_list = [Deltas([Delta(DeltaType.UPDATED, mk_fifo_obj("foo", 6))])]
        for expected in expected_list:
            cur = await pop(f)
            self.assertEqual(cur, expected)

    @async_test
    async def test_has_synced_correct_on_deletion(self):
        f = DeltaFIFO(
            test_fifo_object_key_func,
            known_objects=KeyLookupFunc(
                lambda: [
                    mk_fifo_obj("foo", 5),
                    mk_fifo_obj("bar", 6),
                    mk_fifo_obj("baz", 7),
                ]
            ),
        )
        await f.replace([mk_fifo_obj("foo", 5)], "0")

        expected_list = [
            Deltas([Delta(DeltaType.SYNC, mk_fifo_obj("foo", 5))]),
            Deltas(
                [
                    Delta(
                        DeltaType.DELETED,
                        DeletedFinalStateUnknown("bar", mk_fifo_obj("bar", 6)),
                    )
                ]
            ),
            Deltas(
                [
                    Delta(
                        DeltaType.DELETED,
                        DeletedFinalStateUnknown("baz", mk_fifo_obj("baz", 7)),
                    )
                ]
            ),
        ]
        for expected in expected_list:
            self.assertFalse(f.has_synced())
            cur = await pop(f)
            self.assertEqual(cur, expected)
        self.assertTrue(f.has_synced())

    @async_test
    async def test_detect_line_jumpers(self):
        f = DeltaFIFO(test_fifo_object_key_func)

        await f.add(mk_fifo_obj("foo", 10))
        await f.add(mk_fifo_obj("bar", 1))
        await f.add(mk_fifo_obj("foo", 11))
        await f.add(mk_fifo_obj("foo", 13))
        await f.add(mk_fifo_obj("zab", 30))

        self.assertEqual((await test_pop(f)).val, 13)

        await f.add(mk_fifo_obj("foo", 14))
        self.assertEqual((await test_pop(f)).val, 1)
        self.assertEqual((await test_pop(f)).val, 30)
        self.assertEqual((await test_pop(f)).val, 14)

    @async_test
    async def test_add_if_not_present(self):
        f = DeltaFIFO(test_fifo_object_key_func)

        await f.add(mk_fifo_obj("b", 3))
        b3 = await pop(f)
        await f.add(mk_fifo_obj("c", 4))
        c4 = await pop(f)
        self.assertEqual(len(f._items), 0)

        await f.add(mk_fifo_obj("a", 1))
        await f.add(mk_fifo_obj("b", 2))
        await f.add_if_not_present(b3)
        await f.add_if_not_present(c4)

        self.assertEqual(len(f._items), 3)

        expected_values = [1, 2, 4]
        for expected in expected_values:
            self.assertEqual((await test_pop(f)).val, expected)

    @async_test
    async def test_key_of(self):
        f = DeltaFIFO(test_fifo_object_key_func)

        table = [
            {"obj": TestFifoObject("A"), "key": "A"},
            {"obj": DeletedFinalStateUnknown("B", None), "key": "B"},
            {"obj": TestFifoObject("C"), "key": "C"},
            {"obj": DeletedFinalStateUnknown("D", None), "key": "D"},
        ]

        for item in table:
            got = f.key_of(item["obj"])
            self.assertEqual(got, item["key"])

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
            f = DeltaFIFO(test_fifo_object_key_func)
            for action in test["actions"]:
                await action(f)
            self.assertEqual(f.has_synced(), test["expected_synced"])


async def pop(queue_):
    result = None

    async def process(obj):
        nonlocal result
        result = obj

    await queue_.pop(process)
    return result


def test_fifo_object_key_func(obj):
    return obj.name


class TestFifoObject(NamedTuple):
    name: str
    val: Any = None


def mk_fifo_obj(name, val):
    return TestFifoObject(name, val)


async def test_pop(f):
    return (await pop(f)).newest().object


class KeyLookupFunc:
    def __init__(self, f):
        self._f = f

    def list_keys(self):
        return [fifo_obj.name for fifo_obj in self._f()]

    def get_by_key(self, key):
        for v in self._f():
            if v.name == key:
                return v
        return None


if __name__ == "__main__":
    unittest.main()
