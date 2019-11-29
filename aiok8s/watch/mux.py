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
import enum


class FullChannelBehavior(enum.Enum):
    WAIT_IF_CHANNEL_FULL = 1
    DROP_IF_CHANNEL_FULL = 2


_INCOMING_QUEUE_LENGTH = 25
_INTERNAL_RUN_FUNCTION_MARKER = "internal-do-function"


class Broadcaster:
    def __init__(self, queue_length, full_channel_behavior):
        self._lock = asyncio.Lock()
        self._watchers = {}
        self._next_watcher = 0
        self._distributing = asyncio.Event()
        self._incoming = asyncio.Queue(maxsize=_INCOMING_QUEUE_LENGTH)
        self._watch_queue_length = queue_length
        self._full_channel_behavior = full_channel_behavior
        asyncio.ensure_future(self._loop())

    async def watch(self):
        w = None

        async def f():
            async with self._lock:
                nonlocal w
                id_ = self._next_watcher
                self._next_watcher += 1
                w = _BroadcasterWatcher(
                    asyncio.Queue(maxsize=self._watch_queue_length), id_, self
                )
                self._watchers[id_] = w

        await self._block_queue(f)
        return w

    async def watch_with_prefix(self, queued_events):
        w = None

        async def f():
            async with self._lock:
                nonlocal w
                id_ = self._next_watcher
                self._next_watcher += 1
                n = len(queued_events) + 1
                length = max(self._watch_queue_length, n)
                w = _BroadcasterWatcher(asyncio.Queue(maxsize=length), id_, self)
                self._watchers[id_] = w
                for e in queued_events:
                    await w._put(e)

        await self._block_queue(f)
        return w

    async def action(self, action, obj):
        await self._incoming.put({"type": action, "object": obj})

    async def shutdown(self):
        await self._incoming.put(None)
        await self._distributing.wait()

    async def _block_queue(self, f):
        event = asyncio.Event()

        async def func():
            try:
                await f()
            finally:
                event.set()

        await self._incoming.put(
            {
                "type": _INTERNAL_RUN_FUNCTION_MARKER,
                "object": _FunctionFakeRuntimeObject(func),
            }
        )
        await event.wait()

    async def _stop_watching(self, id_):
        async with self._lock:
            w = self._watchers.pop(id_, None)
            if w:
                w._stopped.set()

    async def _close_all(self):
        async with self._lock:
            for w in self._watchers.values():
                w._stopped.set()
            self._watchers = {}

    async def _loop(self):
        while True:
            event = await self._incoming.get()
            if event is None:
                break
            if event["type"] == _INTERNAL_RUN_FUNCTION_MARKER:
                await event["object"]()
                continue
            await self._distribute(event)
        await self._close_all()
        self._distributing.set()

    async def _distribute(self, event):
        async with self._lock:
            if self._full_channel_behavior is FullChannelBehavior.DROP_IF_CHANNEL_FULL:
                for w in self._watchers.values():
                    if w._stopped.is_set():
                        continue
                    try:
                        w._result.put_nowait(event)
                    except asyncio.QueueFull:
                        pass
                return
            for w in self._watchers.values():
                _, pending = await asyncio.wait(
                    [
                        asyncio.ensure_future(w._put(event)),
                        asyncio.ensure_future(w._stopped.wait()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()


class _FunctionFakeRuntimeObject:
    def __init__(self, f):
        self._f = f

    def __call__(self):
        return self._f()


class _BroadcasterWatcher:
    def __init__(self, result, id_, m):
        self._result = result
        self._stopped = asyncio.Event()
        self._task = asyncio.ensure_future(self._stopped.wait())
        self._id = id_
        self._m = m

    def __aiter__(self):
        return self

    async def __anext__(self):
        event = asyncio.ensure_future(self._result.get())
        while True:
            done, _ = await asyncio.wait(
                [event, self._task], return_when=asyncio.FIRST_COMPLETED
            )
            if event in done:
                if not self._result.maxsize:
                    self._result.task_done()
                return await event
            if self._stopped.is_set():
                raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.stop()

    async def stop(self):
        if not self._stopped.is_set():
            self._stopped.set()
            await self._m._stop_watching(self._id)

    async def _put(self, item):
        await self._result.put(item)
        if not self._result.maxsize:
            await self._result.join()
