import asyncio


class EventType:
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"
    BOOKMARK = "BOOKMARK"
    ERROR = "ERROR"


class FakeWatcher:
    def __init__(self, result):
        self._stopped = asyncio.Event()
        self._task = asyncio.ensure_future(self._stopped.wait())
        self._result = result
        self._mutex = asyncio.Lock()

    def __aiter__(self):
        return self

    async def __anext__(self):
        event = asyncio.ensure_future(self._result.get())
        while True:
            done, _ = await asyncio.wait(
                [event, self._task], return_when=asyncio.FIRST_COMPLETED
            )
            if self._stopped.is_set():
                raise StopAsyncIteration
            if event in done:
                self._result.task_done()
                return await event

    def stop(self):
        self._stopped.set()

    def is_stopped(self):
        return self._stopped.is_set()

    async def reset(self):
        async with self._mutex:
            self._stopped.clear()
            self._task = asyncio.ensure_future(self._stopped.wait())
            self._result = asyncio.Queue()

    async def add(self, obj):
        await self.action(EventType.ADDED, obj)

    async def modify(self, obj):
        await self.action(EventType.MODIFIED, obj)

    async def delete(self, obj):
        await self.action(EventType.DELETED, obj)

    async def error(self, obj):
        await self.action(EventType.ERROR, obj)

    async def action(self, action, obj):
        async with self._mutex:
            await self._result.put({"type": action, "object": obj})
            await self._result.join()


def new_fake():
    return FakeWatcher(asyncio.Queue())
