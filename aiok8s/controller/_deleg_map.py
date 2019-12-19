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

from aiok8s.cache import shared_informer
from aiok8s.controller import _informers_map


class InformersMap:
    def __init__(self, resync, namespace):
        self._structured = _informers_map._SpecificInformersMap(
            resync, namespace, _informers_map._create_structured_list_watch
        )

    async def start(self):
        await self._structured.start()

    async def wait_for_cache_sync(self):
        synced_funcs = self._structured.has_synced_funcs()
        return await shared_informer.wait_for_cache_sync(*synced_funcs)

    async def get(self, gvk, obj):
        return await self._structured.get(gvk, obj)
