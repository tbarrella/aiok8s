import asyncio
import unittest
from typing import NamedTuple

from .mux import Broadcaster, FullChannelBehavior
from .watch import EventType


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


class TestBrodcaster(unittest.TestCase):
    @async_test
    async def test(self):
        table = [
            {"type": EventType.ADDED, "object": MyType("foo", "hello world 1")},
            {"type": EventType.ADDED, "object": MyType("bar", "hello world 2")},
            {"type": EventType.MODIFIED, "object": MyType("foo", "goodbye world 3")},
            {"type": EventType.DELETED, "object": MyType("bar", "hello world 4")},
        ]

        m = Broadcaster(0, FullChannelBehavior.WAIT_IF_CHANNEL_FULL)

        test_watchers = 2
        queue = asyncio.Queue(maxsize=test_watchers)
        for i in range(test_watchers):
            await queue.put(None)

            async def coro(watcher, w):
                table_line = 0
                async for event in w:
                    self.assertEqual(event, table[table_line])
                    table_line += 1
                queue.task_done()

            asyncio.ensure_future(coro(i, await m.watch()))
        for item in table:
            await m.action(item["type"], item["object"])
        await m.shutdown()
        await queue.join()


class MyType(NamedTuple):
    id: str
    value: str


if __name__ == "__main__":
    unittest.main()
