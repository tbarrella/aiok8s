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

import asyncio
import random
from typing import Any, NamedTuple

from kubernetes_asyncio import watch
from kubernetes_asyncio.client import api

from aiok8s.cache import index, list_watch, shared_informer
from aiok8s.controller import _cache_reader


class MapEntry(NamedTuple):
    informer: Any  # SharedIndexInformer
    reader: _cache_reader.CacheReader


class _SpecificInformersMap:
    def __init__(self, resync, namespace, create_lister_watcher):
        self._resync = resync
        self._namespace = namespace
        self._create_lister_watcher = create_lister_watcher
        self._informers_by_gvk = {}
        self._mutex = asyncio.Lock()
        self._started = False

    async def start(self):
        async with self._mutex:
            aws = [
                asyncio.ensure_future(informer.informer.run())
                for informer in self._informers_by_gvk.values()
            ]
            self._started = True
        await asyncio.gather(*aws)

    def has_synced_funcs(self):
        return [
            informer.informer.has_synced for informer in self._informers_by_gvk.values()
        ]

    async def get(self, gvk, obj):
        async with self._mutex:
            informer = self._informers_by_gvk.get(gvk)
            started = self._started

        if not informer:
            informer = self._add_informer_to_map(gvk, obj)

        if started and not informer.informer.has_synced():
            if not await shared_informer.wait_for_cache_sync(
                informer.informer.has_synced
            ):
                raise Exception(f"failed waiting for {obj} Informer to sync")

        return started, informer

    def _add_informer_to_map(self, gvk, obj):
        informer = self._informers_by_gvk.get(gvk)
        if informer:
            return informer

        lister_watcher = self._create_lister_watcher(gvk, self)
        indexers = index.Indexers(
            {index.NAMESPACE_INDEX: index.meta_namespace_index_func}
        )
        new_informer = shared_informer.new_shared_index_informer(
            lister_watcher, obj, _resync_period(self._resync)(), indexers
        )
        informer = MapEntry(
            new_informer, _cache_reader.CacheReader(new_informer.get_indexer(), gvk)
        )
        self._informers_by_gvk[gvk] = informer
        return informer


def _create_structured_list_watch(gvk, ip):
    async def list_func(options):
        lister = _get_lister(ip)
        kwargs = _get_kwargs(ip, options)
        return await lister(**kwargs)

    async def watch_func(options):
        lister = _get_lister(ip)
        kwargs = _get_kwargs(ip, options)
        return watch.Watch().stream(lister, **kwargs)

    return list_watch.ListWatch(list_func, watch_func)


def _resync_period(resync):
    return lambda: resync * (random.random() / 5 * 0.9)


# TODO: Generalize
def _get_lister(ip):
    core = api.CoreV1Api()
    if ip._namespace:
        return core.list_namespaced_pod
    else:
        return core.list_pod_for_all_namespaces


def _get_kwargs(ip, options):
    kwargs = dict(options)
    if ip._namespace:
        kwargs["namespace"] = ip._namespace
    return kwargs
