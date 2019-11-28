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

from aiok8s.api import meta
from aiok8s.cache import index, thread_safe_store


class StoreKeyError(KeyError):
    def __init__(self, obj):
        self.obj = obj


class ExplicitKey(str):
    pass


def meta_namespace_key_func(obj):
    if isinstance(obj, ExplicitKey):
        return str(obj)
    metadata = meta.accessor(obj)
    if metadata.namespace:
        return f"{metadata.namespace}/{metadata.name}"
    return metadata.name


def new_store(key_func):
    return _Cache(
        thread_safe_store.new_thread_safe_store(index.Indexers(), index.Indices()),
        key_func,
    )


def new_indexer(key_func, indexers):
    return _Cache(
        thread_safe_store.new_thread_safe_store(indexers, index.Indices()), key_func
    )


class _Cache:
    def __init__(self, cache_storage, key_func):
        self._cache_storage = cache_storage
        self._key_func = key_func

    async def add(self, obj):
        try:
            key = self._key_func(obj)
        except Exception as e:
            raise StoreKeyError(obj) from e
        await self._cache_storage.add(key, obj)

    async def update(self, obj):
        try:
            key = self._key_func(obj)
        except Exception as e:
            raise StoreKeyError(obj) from e
        await self._cache_storage.update(key, obj)

    async def delete(self, obj):
        try:
            key = self._key_func(obj)
        except Exception as e:
            raise StoreKeyError(obj) from e
        await self._cache_storage.delete(key, obj)

    def list(self):
        return self._cache_storage.list()

    def list_keys(self):
        return self._cache_storage.list_keys()

    def get(self, obj):
        try:
            key = self._key_func(obj)
        except Exception as e:
            raise StoreKeyError(obj) from e
        return self.get_by_key(key)

    def get_by_key(self, key):
        return self._cache_storage.get(key)

    async def replace(self, list_, resource_version):
        items = {}
        for item in list_:
            try:
                key = self._key_func(item)
            except Exception as e:
                raise StoreKeyError(item) from e
            items[key] = item
        await self._cache_storage.replace(items, resource_version)

    async def resync(self):
        await self._cache_storage.resync()

    def get_indexers(self):
        return self._cache_storage.get_indexers()

    async def index(self, index_name, obj):
        return await self._cache_storage.index(index_name, obj)

    async def index_keys(self, index_name, index_key):
        return await self._cache_storage.index_keys(index_name, index_key)

    async def list_index_func_values(self, index_name):
        return await self._cache_storage.list_index_func_values(index_name)

    async def by_index(self, index_name, index_key):
        return await self._cache_storage.by_index(index_name, index_key)

    async def add_indexers(self, new_indexers):
        await self._cache_storage.add_indexers(new_indexers)
