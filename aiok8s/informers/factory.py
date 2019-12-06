# Copyright 2017 The Kubernetes Authors.
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

from kubernetes_asyncio import client, watch
from kubernetes_asyncio.client import models

from aiok8s.cache import index, list_watch, listers, shared_informer


def new(
    client=None,
    *,
    namespace=None,
    tweak_list_options=None,
    default_resync=None,
    custom_resync=None
):
    # Different from `client-go`: Use default from controller-manager
    default_resync = default_resync or _MIN_RESYNC_PERIOD * (1 + random.random())
    custom_resync = custom_resync or {}
    return _SharedInformerFactory(
        client, namespace, tweak_list_options, default_resync, custom_resync
    )


class _SharedInformerFactory:
    def __init__(
        self, client, namespace, tweak_list_options, default_resync, custom_resync
    ):
        self._client = client
        self._namespace = namespace
        self._tweak_list_options = tweak_list_options
        self._default_resync = default_resync
        self._custom_resync = custom_resync
        self._informers = {}
        self._started_informers = set()
        self._stop_tasks = []

    def start(self, stop_event):
        tasks = []
        for informer_type, informer in self._informers.items():
            if informer_type not in self._started_informers:
                tasks.append(asyncio.ensure_future(informer.run()))
                self._started_informers.add(informer_type)

        stop_task = asyncio.ensure_future(_create_stop_task(stop_event, tasks))
        self._stop_tasks.append(stop_task)

    async def wait_for_cache_sync(self):
        informers = {
            informer_type: informer
            for informer_type, informer in self._informers.items()
            if informer_type in self._started_informers
        }
        return {
            informer_type: await shared_informer.wait_for_cache_sync(
                informer.has_synced
            )
            for informer_type, informer in informers.items()
        }

    # Not in `client-go`
    async def join(self):
        await asyncio.gather(*self._stop_tasks)

    def pods(self):
        return _PodInformer(self)

    def _informer_for(self, informer_type, new_func):
        informer = self._informers.get(informer_type)
        if informer:
            return informer

        resync_period = self._custom_resync.get(informer_type, self._default_resync)
        informer = new_func(self._client, resync_period)
        self._informers[informer_type] = informer
        return informer


_MIN_RESYNC_PERIOD = 12 * 60 * 60


async def _create_stop_task(stop_event, tasks):
    await stop_event.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


class _Informer:
    def __init__(self, factory):
        self._factory = factory

    def informer(self):
        return self._factory._informer_for(self._object_type, self._new)

    def lister(self):
        return listers.new_generic_lister(self.informer().get_indexer())

    def _get_lister(self, client):
        api = self._api_cls(client)
        method = self._namespaced_lister if self._factory._namespace else self._lister
        return getattr(api, method)

    def _get_kwargs(self, options):
        kwargs = dict(options)
        if self._factory._tweak_list_options:
            self._factory._tweak_list_options(kwargs)
        if self._factory._namespace:
            kwargs["namespace"] = self._factory._namespace
        return kwargs

    def _new(self, client, resync_period):
        async def list_func(options):
            lister = self._get_lister(client)
            kwargs = self._get_kwargs(options)
            return await lister(**kwargs)

        async def watch_func(options):
            lister = self._get_lister(client)
            kwargs = self._get_kwargs(options)
            return watch.Watch().stream(lister, **kwargs)

        indexers = index.Indexers(
            {index.NAMESPACE_INDEX: index.meta_namespace_index_func}
        )
        return shared_informer.new_shared_index_informer(
            list_watch.ListWatch(list_func, watch_func),
            self._object_type,
            resync_period,
            indexers,
        )


class _PodInformer(_Informer):
    _object_type = models.V1Pod
    _api_cls = client.CoreV1Api
    _namespaced_lister = "list_namespaced_pod"
    _lister = "list_pod_for_all_namespaces"
