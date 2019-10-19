import asyncio


class EventType:
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"
    BOOKMARK = "BOOKMARK"
    ERROR = "ERROR"


class FakeWatcher:
    def __init__(self, result):
        self.stopped = False
        self._result = result
        self._mutex = asyncio.Lock()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.stopped:
            raise StopAsyncIteration
        event = await self._result.get()
        self._result.task_done()
        if event is None:
            raise StopAsyncIteration
        return event

    async def stop(self):
        async with self._mutex:
            if not self.stopped:
                await self._result.put(None)
                self.stopped = True

    def is_stopped(self):
        return self.stopped

    async def reset(self):
        async with self._mutex:
            self.stopped = False
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
        await self._result.put({"type": action, "object": obj})
        await self._result.join()


def new_fake():
    return FakeWatcher(asyncio.Queue())
