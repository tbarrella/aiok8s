import threading
import unittest

from .watch import EventType, new_fake


class TestWatch(unittest.TestCase):
    def test_fake(self):
        f = new_fake()

        table = [
            {"t": EventType.ADDED, "s": TestType("foo")},
            {"t": EventType.MODIFIED, "s": TestType("qux")},
            {"t": EventType.MODIFIED, "s": TestType("bar")},
            {"t": EventType.DELETED, "s": TestType("bar")},
            {"t": EventType.ERROR, "s": TestType("error: blah")},
        ]

        def consumer(w):
            for expect in table:
                got = next(w)
                self.assertEqual(got["type"], expect["t"])
                self.assertEqual(got["object"], expect["s"])
            with self.assertRaises(StopIteration):
                next(w)

        def sender():
            f.add(TestType("foo"))
            f.action(EventType.MODIFIED, TestType("qux"))
            f.modify(TestType("bar"))
            f.delete(TestType("bar"))
            f.error(TestType("error: blah"))
            f.stop()

        threading.Thread(target=sender).start()
        consumer(f)


class TestType(str):
    pass


if __name__ == "__main__":
    unittest.main()
