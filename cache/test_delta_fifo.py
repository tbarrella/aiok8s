import unittest
from multiprocessing.pool import ThreadPool
from queue import Empty, Queue
from typing import Any, NamedTuple

from .delta_fifo import DeltaFIFO
from .fifo import RequeueError


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
            raise RequeueError(err=TestError)

        with self.assertRaises(TestError):
            f.pop(process)
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

        got = Queue(maxsize=2)

        def get_popped():
            while True:
                obj = test_pop(f)
                got.put(obj)

        with ThreadPool() as pool:
            pool.apply_async(get_popped)
            first = got.get()
            self.assertEqual(first.val, 15)
            with self.assertRaises(Empty):
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


def pop(queue):
    result = None

    def process(obj):
        nonlocal result
        result = obj

    queue.pop(process)
    return result


def test_fifo_object_key_func(obj):
    return obj.name


class TestFifoObject(NamedTuple):
    name: str
    val: Any


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
