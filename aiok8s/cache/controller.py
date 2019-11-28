# Copyright 2015 The Kubernetes Authors.
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
from typing import Any, Awaitable, Callable, NamedTuple, Optional

from aiok8s.cache import delta_fifo, fifo, reflector, store
from aiok8s.util import clock, wait

ShouldResyncFunc = Callable[[], bool]
ProcessFunc = Callable[[Any], Awaitable[None]]


class Config(NamedTuple):
    queue: delta_fifo.DeltaFIFO
    # TODO
    lister_watcher: Any
    process: ProcessFunc
    object_type: Any
    full_resync_period: float
    retry_on_error: bool
    should_resync: Optional[ShouldResyncFunc] = None


def new(c):
    return _Controller(c)


class _Controller:
    def __init__(self, config):
        self._config = config
        self._reflector = None
        self._reflector_mutex = asyncio.Lock()
        self._clock = clock.RealClock()

    async def run(self, stop_event):
        async def coro():
            await stop_event.wait()
            await self._config.queue.close()

        asyncio.ensure_future(coro())
        r = reflector.Reflector(
            self._config.lister_watcher,
            self._config.object_type,
            self._config.queue,
            self._config.full_resync_period,
        )
        r.should_resync = self._config.should_resync
        r._clock = self._clock

        async with self._reflector_mutex:
            self._reflector = r

        task = asyncio.ensure_future(r.run(stop_event))
        try:
            await wait.until(self._process_loop, 1, stop_event)
        finally:
            await task

    def has_synced(self):
        return self._config.queue.has_synced()

    def last_sync_resource_version(self):
        return self._reflector.last_sync_resource_version() if self._reflector else ""

    async def _process_loop(self):
        while True:
            try:
                await self._config.queue.pop(self._config.process)
            except fifo.FIFOClosedError:
                return
            except fifo.ProcessError as e:
                if self._config.retry_on_error:
                    await self._config.queue.add_if_not_present(e.item)


class ResourceEventHandlerFuncs(NamedTuple):
    add_func: Optional[Callable[[Any], Awaitable[None]]] = None
    update_func: Optional[Callable[[Any, Any], Awaitable[None]]] = None
    delete_func: Optional[Callable[[Any], Awaitable[None]]] = None

    async def on_add(self, obj):
        if self.add_func:
            await self.add_func(obj)

    async def on_update(self, old_obj, new_obj):
        if self.update_func:
            await self.update_func(old_obj, new_obj)

    async def on_delete(self, obj):
        if self.delete_func:
            await self.delete_func(obj)


def deletion_handling_meta_namespace_key_func(obj):
    if isinstance(obj, delta_fifo.DeletedFinalStateUnknown):
        return obj.key
    return store.meta_namespace_key_func(obj)


def new_informer(lw, obj_type, resync_period, h):
    client_state = store.new_store(deletion_handling_meta_namespace_key_func)
    return client_state, _new_informer(lw, obj_type, resync_period, h, client_state)


def _new_informer(lw, obj_type, resync_period, h, client_state):
    fifo = delta_fifo.DeltaFIFO(
        store.meta_namespace_key_func, known_objects=client_state
    )

    async def process(obj):
        for d in obj:
            if d.type in (
                delta_fifo.DeltaType.SYNC,
                delta_fifo.DeltaType.ADDED,
                delta_fifo.DeltaType.UPDATED,
            ):
                old = client_state.get(d.object)
                if old is not None:
                    await client_state.update(d.object)
                    await h.on_update(old, d.object)
                else:
                    await client_state.add(d.object)
                    await h.on_add(d.object)
            elif d.type == delta_fifo.DeltaType.DELETED:
                await client_state.delete(d.object)
                await h.on_delete(d.object)
            else:
                assert False, f"unexpected type {d.type!r}"

    cfg = Config(
        queue=fifo,
        lister_watcher=lw,
        object_type=obj_type,
        full_resync_period=resync_period,
        retry_on_error=False,
        process=process,
    )
    return new(cfg)
