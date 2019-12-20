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

import functools
from typing import Dict, Hashable, Set, Type

from kubernetes_asyncio import watch
from kubernetes_asyncio.client import api, models

from aiok8s.runtime import schema


# Diverges from Go
class _Client:
    def __init_subclass__(cls):
        super().__init_subclass__()
        if cls.gvk in _GVK_TO_CLIENT_CLS:
            raise AssertionError(f"client for {cls.gvk!r} already defined")
        _GVK_TO_CLIENT_CLS[cls.gvk] = cls
        if cls.object_type:
            _GVK_TO_OBJECT_TYPE[cls.gvk] = cls.object_type
            _OBJECT_TYPE_TO_GVK[cls.object_type] = cls.gvk
        else:
            _UNSTRUCTURED.add(cls.gvk)

    def __init__(self, api_client=None):
        self._api_client = self.api_cls(api_client)

    def _get_watcher(self, namespace):
        lister = self._get_lister(namespace)
        return functools.partial(watch.Watch().stream, lister)


_GVK_TO_CLIENT_CLS: Dict[schema.GroupVersionKind, Type[_Client]] = {}
_GVK_TO_OBJECT_TYPE: Dict[schema.GroupVersionKind, Hashable] = {}
_OBJECT_TYPE_TO_GVK: Dict[Hashable, schema.GroupVersionKind] = {}
_UNSTRUCTURED: Set[schema.GroupVersionKind] = set()


class _PodClient(_Client):
    gvk = schema.GroupVersionKind("", "v1", "Pod")
    object_type = models.V1Pod
    api_cls = api.CoreV1Api

    def _get_lister(self, namespace):
        if namespace:
            return self._api_client.list_namespaced_pod
        else:
            return self._api_client.list_pod_for_all_namespaces


def gvk_for_object(obj):
    return _OBJECT_TYPE_TO_GVK[obj]


def rest_client_for_gvk(gvk):
    return _GVK_TO_CLIENT_CLS[gvk]()
