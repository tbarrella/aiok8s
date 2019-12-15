import asyncio
import time


class Timer:
    def __init__(self, d):
        self.c = asyncio.Queue(maxsize=1)

        async def function():
            await asyncio.sleep(d)
            await self.c.put(time.time())

        self._task = asyncio.ensure_future(function())

    def stop(self):
        self._task.cancel()


class Ticker:
    def __init__(self, d):
        self.c = asyncio.Queue(maxsize=1)

        async def function():
            while True:
                await asyncio.sleep(d)
                try:
                    self.c.put_nowait(time.time())
                except asyncio.QueueFull:
                    pass

        self._task = asyncio.ensure_future(function())

    def stop(self):
        self._task.cancel()
