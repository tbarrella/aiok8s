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
import logging
from typing import Any, NamedTuple

from aiok8s.cache import fifo, store

logger = logging.getLogger(__name__)


class DeltaFIFO:
    def __init__(self, key_func, known_objects=None):
        self._items = {}
        self._queue = []
        self._key_func = key_func
        self._known_objects = known_objects
        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(lock=self._lock)
        self._populated = False
        self._initial_population_count = 0
        self._closed = False

    async def add(self, obj):
        async with self._lock:
            self._populated = True
            return self._queue_action_locked(DeltaType.ADDED, obj)

    async def update(self, obj):
        async with self._lock:
            self._populated = True
            return self._queue_action_locked(DeltaType.UPDATED, obj)

    async def delete(self, obj):
        id_ = self.key_of(obj)
        async with self._lock:
            self._populated = True
            if not self._known_objects:
                if id_ not in self._items:
                    return
            else:
                try:
                    exists = self._known_objects.get_by_key(id_) is not None
                except Exception:
                    pass
                else:
                    item_exists = id_ in self._items
                    if not exists and not item_exists:
                        return
            self._queue_action_locked(DeltaType.DELETED, obj)

    def list(self):
        return [item.newest().object for item in self._items.values()]

    def list_keys(self):
        return list(self._items)

    def get(self, obj):
        key = self.key_of(obj)
        return self.get_by_key(key)

    def get_by_key(self, key):
        d = self._items.get(key)
        return d and _copy_deltas(d)

    async def replace(self, list_, resource_version):
        async with self._lock:
            keys = set()
            for item in list_:
                key = self.key_of(item)
                keys.add(key)
                self._queue_action_locked(DeltaType.SYNC, item)
            if not self._known_objects:
                queued_deletions = 0
                for k, old_item in self._items.items():
                    if k in keys:
                        continue
                    deleted_obj = None
                    n = old_item.newest()
                    if n:
                        deleted_obj = n.object
                    queued_deletions += 1
                    self._queue_action_locked(
                        DeltaType.DELETED, DeletedFinalStateUnknown(k, deleted_obj)
                    )
                if not self._populated:
                    self._populated = True
                    self._initial_population_count = len(list_) + queued_deletions
                return
            known_keys = self._known_objects.list_keys()
            queued_deletions = 0
            for k in known_keys:
                if k in keys:
                    continue
                try:
                    deleted_obj = self._known_objects.get_by_key(k)
                except Exception as e:
                    deleted_obj = None
                    logger.error(
                        "Unexpected error %s during lookup of key %s, "
                        "placing DeleteFinalStateUnknown marker without object",
                        e,
                        k,
                    )
                else:
                    if deleted_obj is None:
                        logger.info(
                            "Key %s does not exist in known objects store, "
                            "placing DeleteFinalStateUnknown marker without object",
                            k,
                        )
                queued_deletions += 1
                self._queue_action_locked(
                    DeltaType.DELETED, DeletedFinalStateUnknown(k, deleted_obj)
                )
            if not self._populated:
                self._populated = True
                self._initial_population_count = len(list_) + queued_deletions

    async def resync(self):
        async with self._lock:
            if not self._known_objects:
                return
            keys = self._known_objects.list_keys()
            for k in keys:
                self._sync_key_locked(k)

    async def pop(self, process):
        async with self._lock:
            while True:
                while not self._queue:
                    if self.is_closed():
                        raise fifo.FIFOClosedError
                    await self._cond.wait()
                id_ = self._queue.pop(0)
                if self._initial_population_count:
                    self._initial_population_count -= 1
                if id_ not in self._items:
                    continue
                item = self._items.pop(id_)
                try:
                    await process(item)
                except fifo.RequeueError as e:
                    self._add_if_not_present(id_, item)
                    if e.__cause__:
                        raise fifo.ProcessError(item) from e.__cause__
                except Exception as e:
                    raise fifo.ProcessError(item) from e
                return item

    async def add_if_not_present(self, obj):
        if not isinstance(obj, Deltas):
            raise TypeError(f"object must be of type Deltas, but got {obj!r}")
        id_ = self.key_of(obj.newest().object)
        async with self._lock:
            self._add_if_not_present(id_, obj)

    def has_synced(self):
        return self._populated and not self._initial_population_count

    async def close(self):
        self._closed = True
        async with self._cond:
            self._cond.notify_all()

    def key_of(self, obj):
        if isinstance(obj, Deltas):
            if not obj:
                raise store.StoreKeyError(obj) from ZeroLengthDeltasObjectError
            obj = obj.newest().object
        if isinstance(obj, DeletedFinalStateUnknown):
            return obj.key
        return self._key_func(obj)

    def is_closed(self):
        return self._closed

    def _add_if_not_present(self, id_, deltas):
        self._populated = True
        if id_ in self._items:
            return
        self._queue.append(id_)
        self._items[id_] = deltas
        self._cond.notify_all()

    def _will_object_be_deleted_locked(self, id_):
        deltas = self._items.get(id_)
        return deltas and deltas[-1].type is DeltaType.DELETED

    def _queue_action_locked(self, action_type, obj):
        id_ = self.key_of(obj)
        if action_type is DeltaType.SYNC and self._will_object_be_deleted_locked(id_):
            return
        new_deltas = Deltas([*self._items.get(id_, []), Delta(action_type, obj)])
        new_deltas = _dedup_deltas(new_deltas)
        if new_deltas:
            if id_ not in self._items:
                self._queue.append(id_)
            self._items[id_] = new_deltas
            self._cond.notify_all()
        else:
            del self._items[id_]

    def _sync_key_locked(self, key):
        try:
            obj = self._known_objects.get_by_key(key)
        except Exception as e:
            logger.error(
                "Unexpected error %s during lookup of key %s, "
                "unable to queue object for sync",
                e,
                key,
            )
            return
        if obj is None:
            logger.info(
                "Key %s does not exist in known objects store, "
                "unable to queue object for sync",
                key,
            )
            return
        id_ = self.key_of(obj)
        if self._items.get(id_):
            return
        self._queue_action_locked(DeltaType.SYNC, obj)


class ZeroLengthDeltasObjectError(Exception):
    pass


class DeltaType(enum.Enum):
    ADDED = "Added"
    UPDATED = "Updated"
    DELETED = "Deleted"
    SYNC = "Sync"


class Delta(NamedTuple):
    type: DeltaType
    object: Any


class Deltas(list):
    def oldest(self):
        return self[0] if self else None

    def newest(self):
        return self[-1] if self else None


class DeletedFinalStateUnknown(NamedTuple):
    key: str
    obj: Any


def _dedup_deltas(deltas):
    n = len(deltas)
    if n < 2:
        return deltas
    a = deltas[n - 1]
    b = deltas[n - 2]
    out = _is_dup(a, b)
    return Deltas([*deltas[: n - 2], out]) if out else deltas


def _is_dup(a, b):
    if b.type is not DeltaType.DELETED or a.type is not DeltaType.DELETED:
        return None
    if isinstance(b.object, DeletedFinalStateUnknown):
        return a
    return b


def _copy_deltas(d):
    return Deltas(d)
