import asyncio
import time


class Timer:
    def __init__(self, d):
        self.c = asyncio.Queue(maxsize=1)

        async def function():
            try:
                await asyncio.sleep(d)
                await self.c.put(time.time())
            except asyncio.CancelledError:
                pass

        self._task = asyncio.ensure_future(function())

    def stop(self):
        self._task.cancel()
