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
import random

from aiok8s.api import meta
from aiok8s.util import clock, wait
from aiok8s.watch import watch

logger = logging.getLogger(__name__)


class Reflector:
    def __init__(self, lw, expected_type, store, resync_period):
        self.should_resync = None
        # TODO: self.watch_list_page_size = 0
        # TODO: name
        self._store = store
        self._lister_watcher = lw
        self._period = 1
        self._resync_period = resync_period
        self._clock = clock.RealClock()
        self._last_sync_resource_version = ""
        self._set_expected_type(expected_type)

    async def run(self, stop_event):
        async def f():
            await self.list_and_watch(stop_event)

        logger.debug(
            "Starting reflector %s (%s)", self._expected_type_name, self._resync_period
        )
        await wait.until(f, self._period, stop_event)

    async def list_and_watch(self, stop_event):
        logger.debug("Listing and watching %s", self._expected_type_name)
        stop_task = asyncio.ensure_future(stop_event.wait())
        options = {"resource_version": "0"}

        list_task = asyncio.ensure_future(self._lister_watcher.list(options))
        await asyncio.wait([list_task, stop_task], return_when=asyncio.FIRST_COMPLETED)
        if stop_event.is_set():
            list_task.cancel()
            return
        list_ = await list_task
        list_meta = meta.list_accessor(list_)
        resource_version = list_meta.resource_version
        items = meta.extract_list(list_)
        await self._sync_with(items, resource_version)
        self._set_last_sync_resource_version(resource_version)

        resync_error_queue = asyncio.Queue(maxsize=1)

        async def resync():
            resync_queue, cleanup = self._resync_queue()
            try:
                while True:
                    resync_queue_task = asyncio.ensure_future(resync_queue.get())
                    await asyncio.wait(
                        [resync_queue_task, stop_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if stop_event.is_set():
                        resync_queue_task.cancel()
                        return
                    if self.should_resync is None or self.should_resync():
                        logger.debug("forcing resync")
                        try:
                            await self._store.resync()
                        except Exception as e:
                            await resync_error_queue.put(e)
                            return
                    cleanup()
                    resync_queue, cleanup = self._resync_queue()
            finally:
                cleanup()

        resync_task = asyncio.ensure_future(resync())
        options = {"resource_version": resource_version}
        try:
            while not stop_event.is_set():
                timeout_seconds = int(_MIN_WATCH_TIMEOUT * (random.random() + 1))
                # TODO: AllowWatchBookmarks
                options["timeout_seconds"] = timeout_seconds
                try:
                    w = await self._lister_watcher.watch(options)
                except Exception as e:
                    # TODO: Handle ECONNREFUSED
                    logger.error("Failed to watch %s: %r", self._expected_type_name, e)
                    return
                try:
                    await self._watch_handler(
                        w, options, resync_error_queue, stop_event
                    )
                except Exception as e:
                    logger.warning(
                        "watch of %s ended with: %r", self._expected_type_name, e
                    )
                    return
        finally:
            tasks = [resync_task, stop_task]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def last_sync_resource_version(self):
        return self._last_sync_resource_version

    def _set_expected_type(self, expected_type):
        self._expected_type = expected_type and type(expected_type)
        if self._expected_type is None:
            self._expected_type_name = _DEFAULT_EXPECTED_TYPE_NAME
            return
        self._expected_type_name = self._expected_type.__name__
        # TODO: Handle Unstructured

    def _resync_queue(self):
        if not self._resync_period:
            return asyncio.Queue(), lambda: False
        t = self._clock.new_timer(self._resync_period)
        return t.c(), t.stop

    async def _sync_with(self, items, resource_version):
        found = list(items)
        await self._store.replace(found, resource_version)

    async def _watch_handler(self, w, options, error_queue, stop_event):
        start = self._clock.now()
        stop_task = asyncio.ensure_future(stop_event.wait())
        event_count = 0
        event_queue = asyncio.Queue()

        async def get_events():
            async with w:
                async for event in w:
                    await event_queue.put(event)
            await event_queue.put(None)

        get_events_task = asyncio.ensure_future(get_events())
        error_task = asyncio.ensure_future(error_queue.get())
        try:
            while True:
                event_task = asyncio.ensure_future(event_queue.get())
                done, _ = await asyncio.wait(
                    [event_task, error_task, stop_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_event.is_set():
                    raise _StopRequestedError
                if error_task in done:
                    raise await error_task
                event = await event_task
                if event is None:
                    break
                if event["type"] == watch.EventType.ERROR:
                    raise Exception
                if self._expected_type is not None and not isinstance(
                    event["object"], self._expected_type
                ):
                    continue
                # TODO: Handle GVK
                try:
                    metadata = meta.accessor(event["object"])
                except AttributeError:
                    continue
                new_resource_version = metadata.resource_version
                if event["type"] == watch.EventType.ADDED:
                    try:
                        await self._store.add(event["object"])
                    except Exception:
                        pass
                elif event["type"] == watch.EventType.MODIFIED:
                    try:
                        await self._store.update(event["object"])
                    except Exception:
                        pass
                elif event["type"] == watch.EventType.DELETED:
                    try:
                        await self._store.delete(event["object"])
                    except Exception:
                        pass
                options["resource_version"] = new_resource_version
                self._set_last_sync_resource_version(new_resource_version)
                event_count += 1
            watch_duration = self._clock.since(start)
            if watch_duration < 1 and not event_count:
                raise Exception(
                    "very short watch: Unexpected watch close - "
                    "watch lasted less than a second and no items received"
                )
            logger.debug(
                "Watch close - %s total %s items received",
                self._expected_type_name,
                event_count,
            )
        finally:
            tasks = [event_task, get_events_task, error_task, stop_task]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _set_last_sync_resource_version(self, v):
        self._last_sync_resource_version = v


_DEFAULT_EXPECTED_TYPE_NAME = "<unspecified>"
_MIN_WATCH_TIMEOUT = 5 * 60


class _StopRequestedError(Exception):
    pass
