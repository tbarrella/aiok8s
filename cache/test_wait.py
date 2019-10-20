import asyncio
import time
import unittest

from .wait import jitter_until, until


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


class TestWait(unittest.TestCase):
    @async_test
    async def test_jitter(self):
        event = asyncio.Event()
        event.set()

        async def f():
            raise Exception("should not have been invoked")

        await until(f, 0, event)

        event = asyncio.Event()
        called = asyncio.Queue()

        async def f():
            await called.put(None)

        async def target():
            await until(f, 0, event)
            await called.put(None)

        asyncio.ensure_future(target())
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

        async def target():
            await jitter_until(f, 0, 1, True, event)
            await called.put(None)

        asyncio.ensure_future(target())
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


if __name__ == "__main__":
    unittest.main()
