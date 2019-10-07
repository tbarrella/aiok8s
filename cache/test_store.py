import unittest
from typing import NamedTuple

from .index import Indexers
from .store import new_indexer, new_store


class TestStore(unittest.TestCase):
    def test_cache(self):
        self.do_test_store(new_store(test_store_key_func))

    def test_index(self):
        self.do_test_index(new_indexer(test_store_key_func, test_store_indexers()))

    def do_test_store(self, store):
        mk_obj = TestStoreObject

        store.add(mk_obj("foo", "bar"))
        item = store.get(mk_obj("foo", ""))
        self.assertEqual(item.val, "bar")
        store.update(mk_obj("foo", "baz"))
        item = store.get(mk_obj("foo", ""))
        self.assertEqual(item.val, "baz")
        store.delete(mk_obj("foo", ""))
        self.assertIsNone(store.get(mk_obj("foo", "")))

        store.add(mk_obj("a", "b"))
        store.add(mk_obj("c", "d"))
        store.add(mk_obj("e", "e"))
        found = {item.val for item in store.list()}
        self.assertGreaterEqual(found, {"b", "d", "e"})
        self.assertEqual(len(found), 3)

        store.replace([mk_obj("foo", "foo"), mk_obj("bar", "bar")], "0")
        found = {item.val for item in store.list()}
        self.assertGreaterEqual(found, {"foo", "bar"})
        self.assertEqual(len(found), 2)

    def do_test_index(self, indexer):
        mk_obj = TestStoreObject

        expected = {"b": {"a", "c"}, "f": {"e"}, "h": {"g"}}
        indexer.add(mk_obj("a", "b"))
        indexer.add(mk_obj("c", "b"))
        indexer.add(mk_obj("e", "f"))
        indexer.add(mk_obj("g", "h"))
        for k, v in expected.items():
            index_results = indexer.index("by_val", mk_obj("", k))
            found = {item.id for item in index_results}
            self.assertGreaterEqual(found, v)


def test_store_key_func(obj):
    return obj.id


def test_store_index_func(obj):
    return [obj.val]


def test_store_indexers():
    return Indexers(by_val=test_store_index_func)


class TestStoreObject(NamedTuple):
    id: str
    val: str


if __name__ == "__main__":
    unittest.main()
