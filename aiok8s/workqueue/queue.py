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
import collections

from aiok8s.util import clock


def new():
    return Type(clock.RealClock())


_DEFAULT_UNFINISHED_WORK_UPDATE_PERIOD = 0.5


class Type:
    def __init__(self, c, *, update_period=_DEFAULT_UNFINISHED_WORK_UPDATE_PERIOD):
        self._clock = c
        self._dirty = set()
        self._processing = set()
        self._cond = asyncio.Condition()
        self._unfinished_work_update_period = update_period
        self._queue = collections.deque()
        self._shutting_down = False

    def __len__(self):
        return len(self._queue)

    async def add(self, item):
        async with self._cond:
            if self._shutting_down:
                return
            if item in self._dirty:
                return
            self._dirty.add(item)
            if item in self._processing:
                return
            self._queue.append(item)
            self._cond.notify()

    async def get(self):
        async with self._cond:
            while not self._queue and not self._shutting_down:
                await self._cond.wait()
            if not self._queue:
                return None, True
            item = self._queue.popleft()
            self._processing.add(item)
            self._dirty.remove(item)
            return item, False

    async def done(self, item):
        async with self._cond:
            self._processing.remove(item)
            if item in self._dirty:
                self._queue.append(item)
                self._cond.notify()

    async def shut_down(self):
        async with self._cond:
            self._shutting_down = True
            self._cond.notify_all()

    def shutting_down(self):
        return self._shutting_down
