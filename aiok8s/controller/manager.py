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

from aiok8s.controller import cache as _cache


def new():
    cache = _cache.new()
    return _ControllerManager(cache=cache)


class _ControllerManager:
    def __init__(self, cache):
        self._cache = cache
        self._non_leader_election_runnables = []
        self._mutex = asyncio.Lock()
        self._started = False

    def add(self, runnable):
        self.set_fields(runnable)

        self._non_leader_election_runnables.append(runnable)

    def set_fields(self, i):
        if hasattr(i, "inject_cache"):
            i.inject_cache(self._cache)
        if hasattr(i, "inject_func"):
            i.inject_func(self.set_fields)

    async def start(self):
        await self._start_non_leader_election_runnables()

    def get_cache(self):
        return self._cache

    async def _start_non_leader_election_runnables(self):
        async with self._mutex:
            task = await self._wait_for_cache()
            aws = [
                asyncio.ensure_future(controller.start())
                for controller in self._non_leader_election_runnables
            ]
        await asyncio.gather(task, *aws)

    async def _wait_for_cache(self):
        if self._started:
            return
        task = asyncio.ensure_future(self._cache.start())
        await self._cache.wait_for_cache_sync()
        self._started = True
        return task
