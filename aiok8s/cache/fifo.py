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

from aiok8s.cache import store


class FIFOClosedError(Exception):
    pass


class RequeueError(Exception):
    pass


class ProcessError(Exception):
    def __init__(self, item):
        self.item = item


class FIFO:
    def __init__(self, key_func):
        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(lock=self._lock)
        self._items = {}
        self._queue = []
        self._populated = False
        self._initial_population_count = 0
        self._key_func = key_func
        self._closed = False

    async def add(self, obj):
        try:
            id_ = self._key_func(obj)
        except Exception as e:
            raise store.StoreKeyError(obj) from e
        async with self._lock:
            self._populated = True
            if id_ not in self._items:
                self._queue.append(id_)
            self._items[id_] = obj
            self._cond.notify_all()

    async def update(self, obj):
        await self.add(obj)

    async def delete(self, obj):
        try:
            id_ = self._key_func(obj)
        except Exception as e:
            raise store.StoreKeyError(obj) from e
        async with self._lock:
            self._populated = True
            del self._items[id_]

    def list(self):
        return list(self._items.values())

    def list_keys(self):
        return list(self._items)

    def get(self, obj):
        try:
            key = self._key_func(obj)
        except Exception as e:
            raise store.StoreKeyError(obj) from e
        return self.get_by_key(key)

    def get_by_key(self, key):
        return self._items.get(key)

    async def replace(self, list_, resource_version):
        items = {}
        for item in list_:
            try:
                key = self._key_func(item)
            except Exception as e:
                raise store.StoreKeyError(item) from e
            items[key] = item
        async with self._lock:
            if not self._populated:
                self._populated = True
                self._initial_population_count = len(items)

            self._items = items
            self._queue = list(items)
            if self._queue:
                self._cond.notify_all()

    async def resync(self):
        async with self._lock:
            in_queue = set(self._queue)
            for id_ in self._items:
                if id_ not in in_queue:
                    self._queue.append(id_)
            if self._queue:
                self._cond.notify_all()

    async def pop(self, process):
        async with self._lock:
            while True:
                while not self._queue:
                    if self.is_closed():
                        raise FIFOClosedError
                    await self._cond.wait()
                id_ = self._queue.pop(0)
                if self._initial_population_count:
                    self._initial_population_count -= 1
                if id_ not in self._items:
                    continue
                item = self._items.pop(id_)
                try:
                    process(item)
                except RequeueError as e:
                    self._add_if_not_present(id_, item)
                    if e.__cause__:
                        raise ProcessError(item) from e.__cause__
                except Exception as e:
                    raise ProcessError(item) from e
                return item

    async def add_if_not_present(self, obj):
        try:
            id_ = self._key_func(obj)
        except Exception as e:
            raise store.StoreKeyError(obj) from e
        async with self._lock:
            self._add_if_not_present(id_, obj)

    def has_synced(self):
        return self._populated and not self._initial_population_count

    async def close(self):
        self._closed = True
        async with self._cond:
            self._cond.notify_all()

    def is_closed(self):
        return self._closed

    def _add_if_not_present(self, id_, obj):
        self._populated = True
        if id_ in self._items:
            return
        self._queue.append(id_)
        self._items[id_] = obj
        self._cond.notify_all()
