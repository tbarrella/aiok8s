import asyncio
import unittest

from .watch import EventType, new_fake


def async_test(coro):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coro(*args, **kwargs))

    return wrapper


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

        asyncio.ensure_future(sender())
        await consumer(f)


class TestType(str):
    pass


if __name__ == "__main__":
    unittest.main()
