import asyncio
import time
import unittest

from .clock import FakeClock, FakePassiveClock


def run(main, *, debug=False):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        loop.set_debug(debug)
        return loop.run_until_complete(main)
    finally:
        try:
            _cancel_all_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def _cancel_all_tasks(loop):
    to_cancel = asyncio.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    loop.run_until_complete(
        asyncio.gather(*to_cancel, loop=loop, return_exceptions=True)
    )

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during asyncio.run() shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )


def async_test(coro):
    def wrapper(*args, **kwargs):
        return run(coro(*args, **kwargs))

    return wrapper


class TestClock(unittest.TestCase):
    @async_test
    async def test_fake_passive_clock(self):
        start_time = time.time()
        tc = FakePassiveClock(start_time)
        await self.exercise_passive_clock(tc)

    @async_test
    async def test_fake_clock(self):
        start_time = time.time()
        tc = FakeClock(start_time)
        await self.exercise_passive_clock(tc)
        await tc.set_time(start_time)
        await tc.step(1)
        now = tc.now()
        self.assertEqual(now - start_time, 1)

    @async_test
    async def test_fake_timer(self):
        tc = FakeClock(time.time())
        self.assertFalse(tc.has_waiters())
        one_sec = tc.new_timer(1)
        two_sec = tc.new_timer(2)
        tre_sec = tc.new_timer(3)
        self.assertTrue(tc.has_waiters())
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        await tc.step(0.999999)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        await tc.step(0.000001)
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        one_sec.c().get_nowait()
        await tc.step(0.000001)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        self.assertFalse(one_sec.stop())
        self.assertTrue(two_sec.stop())
        await tc.step(1)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        self.assertFalse(two_sec.reset(1))
        self.assertTrue(tre_sec.reset(1))
        await tc.step(0.999999)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        await tc.step(0.000001)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        tre_sec.c().get_nowait()

    async def exercise_passive_clock(self, pc):
        t1 = time.time()
        t2 = t1 + 3600
        await pc.set_time(t1)
        tx = pc.now()
        self.assertEqual(tx, t1)
        dx = pc.since(t1)
        self.assertEqual(dx, 0)
        await pc.set_time(t2)
        dx = pc.since(t1)
        self.assertEqual(dx, 3600)
        tx = pc.now()
        self.assertEqual(tx, t2)


if __name__ == "__main__":
    unittest.main()
