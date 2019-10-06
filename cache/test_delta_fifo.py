import unittest
from multiprocessing.pool import ThreadPool
from typing import Any, NamedTuple

from .delta_fifo import DeltaFIFO


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


if __name__ == "__main__":
    unittest.main()
