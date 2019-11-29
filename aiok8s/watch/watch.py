# Copyright 2014 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging

logger = logging.getLogger(__name__)


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
            if event in done:
                self._result.task_done()
                return await event
            if self._stopped.is_set():
                event.cancel()
                raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.stop()

    async def stop(self):
        logger.debug("Stopping fake watcher.")
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
