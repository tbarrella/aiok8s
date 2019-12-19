# Copyright 2018 The Kubernetes Authors.
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

from aiok8s.controller import _deleg_map
from aiok8s.runtime import schema

_DEFAULT_RESYNC_TIME = 10 * 3600


def new(*, namespace=None, resync=_DEFAULT_RESYNC_TIME):
    informers_map = _deleg_map.InformersMap(resync, namespace)
    return _InformerCache(informers_map)


class CacheNotStartedError(Exception):
    pass


class _InformerCache:
    def __init__(self, informers_map):
        self._informers_map = informers_map

    async def get(self, key, out):
        # TODO: Generalize
        gvk = schema.GroupVersionKind("", "v1", "Pod")
        started, cache = await self._informers_map.get(gvk, out)
        if not started:
            raise CacheNotStartedError
        return cache.reader.get(key)

    def list(self, options):
        pass

    async def get_informer(self, obj):
        # TODO: Generalize
        gvk = schema.GroupVersionKind("", "v1", "Pod")
        _, informer = await self._informers_map.get(gvk, obj)
        return informer.informer

    def get_informer_for_kind(self, gvk):
        pass

    async def start(self):
        await self._informers_map.start()

    async def wait_for_cache_sync(self):
        return await self._informers_map.wait_for_cache_sync()

    def index_field(self, obj, field, extract_value):
        pass
