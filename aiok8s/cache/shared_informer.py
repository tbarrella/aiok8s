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
import collections
import logging
from typing import Any, NamedTuple

from aiok8s.cache import controller, delta_fifo, index, store
from aiok8s.util import clock, retry, wait

logger = logging.getLogger(__name__)


def new_shared_informer(lw, obj_type, resync_period):
    return new_shared_index_informer(lw, obj_type, resync_period, index.Indexers())


def new_shared_index_informer(
    lw, obj_type, default_event_handler_resync_period, indexers
):
    return _SharedIndexInformer(
        indexer=store.new_indexer(
            controller.deletion_handling_meta_namespace_key_func, indexers
        ),
        lister_watcher=lw,
        object_type=obj_type,
        default_event_handler_resync_period=default_event_handler_resync_period,
    )


async def wait_for_cache_sync(stop_event, *cache_syncs):
    def condition():
        return all(sync_func() for sync_func in cache_syncs)

    try:
        await wait.poll_immediate_until(_SYNCED_POLL_PERIOD, condition, stop_event)
    except Exception:
        logger.info("stop requested")
        return False
    logger.debug("caches populated")
    return True


_SYNCED_POLL_PERIOD = 0.1
_MINIMUM_RESYNC_PERIOD = 1


class _SharedIndexInformer:
    def __init__(
        self,
        *,
        indexer,
        lister_watcher,
        object_type,
        default_event_handler_resync_period,
    ):
        real_clock = clock.RealClock()
        self._processor = _SharedProcessor(real_clock)
        self._indexer = indexer
        self._lister_watcher = lister_watcher
        self._object_type = object_type
        self._resync_check_period = default_event_handler_resync_period
        self._default_event_handler_resync_period = default_event_handler_resync_period
        self._clock = real_clock
        self._started = False
        self._stopped = False
        self._started_lock = asyncio.Lock()
        self._block_deltas = asyncio.Lock()

    async def add_event_handler(self, handler):
        await self.add_event_handler_with_resync_period(
            handler, self._default_event_handler_resync_period
        )

    async def add_event_handler_with_resync_period(self, handler, resync_period):
        async with self._started_lock:
            if self._stopped:
                logger.info(
                    "Handler %s was not added to shared informer "
                    "because it has stopped already",
                    handler,
                )
                return
            if resync_period:
                if resync_period < _MINIMUM_RESYNC_PERIOD:
                    logger.warning(
                        "resync_period %s is too small. "
                        "Changing it to the minimum allowed value of %s",
                        resync_period,
                        _MINIMUM_RESYNC_PERIOD,
                    )
                    resync_period = _MINIMUM_RESYNC_PERIOD
                if resync_period < self._resync_check_period:
                    if self._started:
                        logger.warning(
                            "resync_period %s is smaller than resync_check_period %s "
                            "and the informer has already started. Changing it to %s",
                            resync_period,
                            self._resync_check_period,
                            self._resync_check_period,
                        )
                        resync_period = self._resync_check_period
                    else:
                        self._resync_check_period = resync_period
                        self._processor._resync_check_period_changed(resync_period)

            listener = _ProcessListener(
                handler,
                resync_period,
                _determine_resync_period(resync_period, self._resync_check_period),
                self._clock.now(),
            )
            if not self._started:
                await self._processor._add_listener(listener)
                return

            async with self._block_deltas:
                await self._processor._add_listener(listener)
                for item in self._indexer.list():
                    await listener._add(_AddNotification(new_obj=item))

    async def run(self, stop_event):
        fifo = delta_fifo.DeltaFIFO(store.meta_namespace_key_func, self._indexer)
        cfg = controller.Config(
            queue=fifo,
            lister_watcher=self._lister_watcher,
            object_type=self._object_type,
            full_resync_period=self._resync_check_period,
            retry_on_error=False,
            should_resync=self._processor._should_resync,
            process=self.handle_deltas,
        )

        async with self._started_lock:
            self._controller = controller.new(cfg)
            self._controller._clock = self._clock
            self._started = True

        processor_stop_event = asyncio.Event()
        task = asyncio.ensure_future(self._processor._run(processor_stop_event))
        try:
            await self._controller.run(stop_event)
        finally:
            async with self._started_lock:
                self._stopped = True
            processor_stop_event.set()
            await task

    def has_synced(self):
        return bool(self._controller) and self._controller.has_synced()

    def last_sync_resource_version(self):
        return self._controller.last_sync_resource_version() if self._controller else ""

    async def add_indexers(self, indexers):
        async with self._started_lock:
            if self._started:
                raise Exception("informer has already started")
            await self._indexer.add_indexers(indexers)

    def get_indexer(self):
        return self._indexer

    async def handle_deltas(self, obj):
        async with self._block_deltas:
            for d in obj:
                if d.type in (
                    delta_fifo.DeltaType.SYNC,
                    delta_fifo.DeltaType.ADDED,
                    delta_fifo.DeltaType.UPDATED,
                ):
                    is_sync = d.type == delta_fifo.DeltaType.SYNC
                    old = self._indexer.get(d.object)
                    if old is not None:
                        await self._indexer.update(d.object)
                        await self._processor._distribute(
                            _UpdateNotification(old_obj=old, new_obj=d.object), is_sync
                        )
                    else:
                        await self._indexer.add(d.object)
                        await self._processor._distribute(
                            _AddNotification(new_obj=d.object), is_sync
                        )
                elif d.type == delta_fifo.DeltaType.DELETED:
                    await self._indexer.delete(d.object)
                    await self._processor._distribute(
                        _DeleteNotification(old_obj=d.object), False
                    )
                else:
                    assert False, f"unexpected type {d.type!r}"


class _UpdateNotification(NamedTuple):
    old_obj: Any
    new_obj: Any


class _AddNotification(NamedTuple):
    new_obj: Any


class _DeleteNotification(NamedTuple):
    old_obj: Any


def _determine_resync_period(desired, check):
    if not desired:
        return 0
    if not check:
        logger.warning(
            "The specified resync_period %s is invalid "
            "because this shared informer doesn't support resyncing",
            desired,
        )
        return 0
    if desired < check:
        logger.warning(
            "The specified resync_period %s is being increased "
            "to the minimum resync_check_period %s",
            desired,
            check,
        )
        return check
    return desired


class _SharedProcessor:
    def __init__(self, clock):
        self._clock = clock
        self._listeners_started = False
        self._listeners_lock = asyncio.Lock()
        self._listeners = []
        self._syncing_listeners = []
        self._tasks = []

    async def _add_listener(self, listener):
        async with self._listeners_lock:
            self._add_listener_locked(listener)
            if self._listeners_started:
                self._tasks.append(asyncio.ensure_future(listener._run()))
                self._tasks.append(asyncio.ensure_future(listener._pop()))

    def _add_listener_locked(self, listener):
        self._listeners.append(listener)
        self._syncing_listeners.append(listener)

    async def _distribute(self, obj, sync):
        async with self._listeners_lock:
            if sync:
                for listener in self._syncing_listeners:
                    await listener._add(obj)
            else:
                for listener in self._listeners:
                    await listener._add(obj)

    async def _run(self, stop_event):
        async with self._listeners_lock:
            for listener in self._listeners:
                self._tasks.append(asyncio.ensure_future(listener._run()))
                self._tasks.append(asyncio.ensure_future(listener._pop()))
            self._listeners_started = True
        await stop_event.wait()
        async with self._listeners_lock:
            for listener in self._listeners:
                listener._stop_add.set()
            await asyncio.gather(*self._tasks)

    def _should_resync(self):
        self._syncing_listeners = []
        resync_needed = False
        now = self._clock.now()
        for listener in self._listeners:
            if listener._should_resync(now):
                resync_needed = True
                self._syncing_listeners.append(listener)
                listener._determine_next_resync(now)
        return resync_needed

    def _resync_check_period_changed(self, resync_check_period):
        for listener in self._listeners:
            resync_period = _determine_resync_period(
                listener._requested_resync_period, resync_check_period
            )
            listener._set_resync_period(resync_period)


class _ProcessListener:
    def __init__(self, handler, requested_resync_period, resync_period, now):
        self._next_queue = asyncio.Queue(maxsize=1)
        self._stop_next = asyncio.Event()
        self._add_queue = asyncio.Queue(maxsize=1)
        self._stop_add = asyncio.Event()
        self._handler = handler
        self._pending_notifications = collections.deque()
        self._requested_resync_period = requested_resync_period
        self._resync_period = resync_period
        self._resync_lock = asyncio.Lock()
        self._determine_next_resync(now)

    async def _add(self, notification):
        await self._add_queue.put(notification)
        await self._add_queue.join()

    async def _pop(self):
        async def get():
            notification_to_add = await self._add_queue.get()
            self._add_queue.task_done()
            return notification_to_add

        next_queue = None
        notification = None
        stop_task = asyncio.ensure_future(self._stop_add.wait())
        try:
            while True:
                get_task = asyncio.ensure_future(get())
                put_task = None
                tasks = [get_task, stop_task]
                if next_queue:
                    put_task = asyncio.ensure_future(next_queue.put(notification))
                    tasks.append(put_task)

                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                if self._stop_add.is_set():
                    for task in pending:
                        task.cancel()
                    return
                if get_task in done:
                    notification_to_add = await get_task
                    if notification is None:
                        notification = notification_to_add
                        next_queue = self._next_queue
                    else:
                        self._pending_notifications.append(notification_to_add)
                else:
                    get_task.cancel()
                if put_task in done:
                    await next_queue.join()
                    if self._pending_notifications:
                        notification = self._pending_notifications.popleft()
                    else:
                        notification = None
                        next_queue = None
                elif put_task:
                    put_task.cancel()
        finally:
            self._stop_next.set()
            stop_task.cancel()

    async def _run(self):
        stop_event = asyncio.Event()
        stop_next_task = asyncio.ensure_future(self._stop_next.wait())

        async def get():
            notification = await self._next_queue.get()
            self._next_queue.task_done()
            return notification

        async def condition():
            while True:
                get_task = asyncio.ensure_future(get())
                await asyncio.wait(
                    [get_task, stop_next_task], return_when=asyncio.FIRST_COMPLETED
                )
                if self._stop_next.is_set():
                    get_task.cancel()
                    return True
                notification = await get_task
                if isinstance(notification, _UpdateNotification):
                    await self._handler.on_update(
                        notification.old_obj, notification.new_obj
                    )
                elif isinstance(notification, _AddNotification):
                    await self._handler.on_add(notification.new_obj)
                elif isinstance(notification, _DeleteNotification):
                    await self._handler.on_delete(notification.old_obj)
                else:
                    logger.error("unrecognized notification: %r", notification)

        async def f():
            try:
                await wait.exponential_backoff(retry.DEFAULT_RETRY, condition)
            except Exception:
                return
            stop_event.set()

        await wait.until(f, 60, stop_event)
        stop_next_task.cancel()

    def _should_resync(self, now):
        return bool(self._resync_period) and now >= self._next_resync

    def _determine_next_resync(self, now):
        self._next_resync = now + self._resync_period

    def _set_resync_period(self, resync_period):
        self._resync_period = resync_period
