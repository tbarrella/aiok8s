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
import copy
import random
from typing import NamedTuple

from aiok8s.api import meta
from aiok8s.watch import mux, watch


class FakeControllerSource:
    def __init__(self):
        self.items = {}
        self.broadcaster = mux.Broadcaster(
            100, mux.FullChannelBehavior.WAIT_IF_CHANNEL_FULL
        )
        self._lock = asyncio.Lock()
        self._changes = []

    async def add(self, obj):
        await self.change({"type": watch.EventType.ADDED, "object": obj}, 1)

    async def modify(self, obj):
        await self.change({"type": watch.EventType.MODIFIED, "object": obj}, 1)

    async def delete(self, last_value):
        await self.change({"type": watch.EventType.DELETED, "object": last_value}, 1)

    async def add_drop_watch(self, obj):
        await self.change({"type": watch.EventType.ADDED, "object": obj}, 0)

    async def modify_drop_watch(self, obj):
        await self.change({"type": watch.EventType.MODIFIED, "object": obj}, 0)

    async def delete_drop_watch(self, last_value):
        await self.change({"type": watch.EventType.DELETED, "object": last_value}, 0)

    async def change(self, e, watch_probability):
        async with self._lock:
            accessor = meta.accessor(e["object"])
            resource_version = len(self._changes) + 1
            accessor.resource_version = str(resource_version)
            self._changes.append(e)
            key = self._key(accessor)
            if e["type"] in (watch.EventType.ADDED, watch.EventType.MODIFIED):
                self.items[key] = e["object"]
            elif e["type"] == watch.EventType.DELETED:
                del self.items[key]
            if random.random() < watch_probability:
                await self.broadcaster.action(e["type"], e["object"])

    async def list(self, options):
        async with self._lock:
            list_ = self._get_list_items_locked()
            list_obj = _List()
            meta.set_list(list_obj, list_)
            list_accessor = meta.list_accessor(list_obj)
            resource_version = len(self._changes)
            list_accessor.resource_version = str(resource_version)
            return list_obj

    async def watch(self, options):
        async with self._lock:
            rc = int(options["resource_version"])
            if rc < len(self._changes):
                changes = [
                    {"type": c["type"], "object": copy.deepcopy(c["object"])}
                    for c in self._changes[rc:]
                ]
                return await self.broadcaster.watch_with_prefix(changes)
            if rc > len(self._changes):
                raise Exception(
                    "resource version in the future not supported by this fake"
                )
            return await self.broadcaster.watch()

    async def shutdown(self):
        await self._lock.acquire()
        await self.broadcaster.shutdown()

    @classmethod
    def _key(cls, accessor):
        return _NNU(accessor.namespace, accessor.name, accessor.uid)

    def _get_list_items_locked(self):
        return list(map(copy.deepcopy, self.items.values()))


class _Metadata:
    pass


class _List:
    def __init__(self):
        self.metadata = _Metadata()


class _NNU(NamedTuple):
    namespace: str
    name: str
    uid: str
