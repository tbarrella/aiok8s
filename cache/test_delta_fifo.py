import queue
import unittest
from multiprocessing.pool import ThreadPool
from typing import Any, NamedTuple

from .delta_fifo import DeletedFinalStateUnknown, Delta, DeltaFIFO, Deltas, DeltaType
from .fifo import ProcessError, RequeueError


class TestDeltaFIFO(unittest.TestCase):
    def test_basic(self):
        f = DeltaFIFO(test_fifo_object_key_func)
        amount = 500

        def add_ints():
            for i in range(amount):
                f.add(mk_fifo_obj(f"a{i}", i + 1))

        def add_floats():
            for i in range(amount):
                f.add(mk_fifo_obj(f"b{i}", i + 1.0))

        last_int = 0
        last_float = 0.0
        with ThreadPool() as pool:
            pool.apply_async(add_ints)
            pool.apply_async(add_floats)
            for i in range(amount * 2):
                obj = test_pop(f).val
                if isinstance(obj, int):
                    self.assertGreater(obj, last_int)
                    last_int = obj
                elif isinstance(obj, float):
                    self.assertGreater(obj, last_float)
                    last_float = obj
                else:
                    assert False, f"unexpected type {obj!r}"

    def test_requeue_on_pop(self):
        f = DeltaFIFO(test_fifo_object_key_func)
        f.add(mk_fifo_obj("foo", 10))

        def process(obj):
            self.assertEqual(obj[0].object.name, "foo")
            raise RequeueError

        f.pop(process)
        f.get_by_key("foo")

        class TestError(Exception):
            pass

        def process(obj):
            self.assertEqual(obj[0].object.name, "foo")
            raise RequeueError from TestError

        try:
            f.pop(process)
        except ProcessError as e:
            self.assertIsInstance(e.__cause__, TestError)
        else:
            assert False, "expected error"
        f.get_by_key("foo")

        def process(obj):
            self.assertEqual(obj[0].object.name, "foo")

        f.pop(process)
        self.assertIsNone(f.get_by_key("foo"))

    def test_add_update(self):
        f = DeltaFIFO(test_fifo_object_key_func)
        f.add(mk_fifo_obj("foo", 10))
        f.update(mk_fifo_obj("foo", 12))
        f.delete(mk_fifo_obj("foo", 15))

        self.assertEqual(f.list(), [mk_fifo_obj("foo", 15)])
        self.assertEqual(f.list_keys(), ["foo"])

        got = queue.Queue(maxsize=2)

        def get_popped():
            while True:
                obj = test_pop(f)
                got.put(obj)

        with ThreadPool() as pool:
            pool.apply_async(get_popped)
            first = got.get()
            self.assertEqual(first.val, 15)
            with self.assertRaises(queue.Empty):
                got.get(timeout=0.05)
            self.assertIsNone(f.get(mk_fifo_obj("foo", "")))

    def test_enqueueing_no_lister(self):
        f = DeltaFIFO(test_fifo_object_key_func)
        f.add(mk_fifo_obj("foo", 10))
        f.update(mk_fifo_obj("bar", 15))
        f.add(mk_fifo_obj("qux", 17))
        f.delete(mk_fifo_obj("qux", 18))

        f.delete(mk_fifo_obj("baz", 20))

        expect_list = [10, 15, 18]
        for expect in expect_list:
            self.assertEqual(test_pop(f).val, expect)
        self.assertEqual(len(f._items), 0)

    def test_enqueueing_with_lister(self):
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
        f.add(mk_fifo_obj("foo", 10))
        f.update(mk_fifo_obj("bar", 15))

        f.delete(mk_fifo_obj("baz", 20))

        expect_list = [10, 15, 20]
        for expect in expect_list:
            self.assertEqual(test_pop(f).val, expect)
        self.assertEqual(len(f._items), 0)

    def test_add_replace(self):
        f = DeltaFIFO(test_fifo_object_key_func)
        f.add(mk_fifo_obj("foo", 10))
        f.replace([mk_fifo_obj("foo", 15)], "0")
        got = queue.Queue(maxsize=2)

        def get_popped():
            while True:
                got.put(test_pop(f))

        with ThreadPool() as pool:
            pool.apply_async(get_popped)
            first = got.get()
            self.assertEqual(first.val, 15)
            with self.assertRaises(queue.Empty):
                got.get(timeout=0.05)
            self.assertIsNone(f.get(mk_fifo_obj("foo", "")))

    def test_resync_non_existing(self):
        f = DeltaFIFO(
            test_fifo_object_key_func,
            known_objects=KeyLookupFunc(lambda: [mk_fifo_obj("foo", 5)]),
        )
        f.delete(mk_fifo_obj("foo", 10))
        f.resync()

        deltas = f._items["foo"]
        self.assertEqual(len(deltas), 1)
        self.assertIs(deltas[0].type, DeltaType.DELETED)

    def test_delete_existing_non_propagated(self):
        f = DeltaFIFO(
            test_fifo_object_key_func, known_objects=KeyLookupFunc(lambda: [])
        )
        f.add(mk_fifo_obj("foo", 5))
        f.delete(mk_fifo_obj("foo", 6))

        deltas = f._items["foo"]
        self.assertEqual(len(deltas), 2)
        self.assertIs(deltas[-1].type, DeltaType.DELETED)

    def test_replace_makes_deletions(self):
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
        f.delete(mk_fifo_obj("baz", 10))
        f.replace([mk_fifo_obj("foo", 5)], "0")

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
            cur = pop(f)
            self.assertEqual(cur, expected)

    def test_update_resync_race(self):
        f = DeltaFIFO(
            test_fifo_object_key_func,
            known_objects=KeyLookupFunc(lambda: [mk_fifo_obj("foo", 5)]),
        )
        f.update(mk_fifo_obj("foo", 6))
        f.resync()

        expected_list = [Deltas([Delta(DeltaType.UPDATED, mk_fifo_obj("foo", 6))])]
        for expected in expected_list:
            cur = pop(f)
            self.assertEqual(cur, expected)

    def test_has_synced_correct_on_deletion(self):
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
        f.replace([mk_fifo_obj("foo", 5)], "0")

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
            cur = pop(f)
            self.assertEqual(cur, expected)
        self.assertTrue(f.has_synced())

    def test_detect_line_jumpers(self):
        f = DeltaFIFO(test_fifo_object_key_func)

        f.add(mk_fifo_obj("foo", 10))
        f.add(mk_fifo_obj("bar", 1))
        f.add(mk_fifo_obj("foo", 11))
        f.add(mk_fifo_obj("foo", 13))
        f.add(mk_fifo_obj("zab", 30))

        self.assertEqual(test_pop(f).val, 13)

        f.add(mk_fifo_obj("foo", 14))
        self.assertEqual(test_pop(f).val, 1)
        self.assertEqual(test_pop(f).val, 30)
        self.assertEqual(test_pop(f).val, 14)

    def test_add_if_not_present(self):
        f = DeltaFIFO(test_fifo_object_key_func)

        f.add(mk_fifo_obj("b", 3))
        b3 = pop(f)
        f.add(mk_fifo_obj("c", 4))
        c4 = pop(f)
        self.assertEqual(len(f._items), 0)

        f.add(mk_fifo_obj("a", 1))
        f.add(mk_fifo_obj("b", 2))
        f.add_if_not_present(b3)
        f.add_if_not_present(c4)

        self.assertEqual(len(f._items), 3)

        expected_values = [1, 2, 4]
        for expected in expected_values:
            self.assertEqual(test_pop(f).val, expected)

    def test_key_of(self):
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

    def test_has_synced(self):
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
                action(f)
            self.assertEqual(f.has_synced(), test["expected_synced"])


def pop(queue_):
    result = None

    def process(obj):
        nonlocal result
        result = obj

    queue_.pop(process)
    return result


def test_fifo_object_key_func(obj):
    return obj.name


class TestFifoObject(NamedTuple):
    name: str
    val: Any = None


def mk_fifo_obj(name, val):
    return TestFifoObject(name, val)


def test_pop(f):
    return pop(f).newest().object


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
