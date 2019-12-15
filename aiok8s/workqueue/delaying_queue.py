# Copyright 2016 The Kubernetes Authors.
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
import heapq
import itertools
from typing import Hashable, NamedTuple

from aiok8s.util import clock
from aiok8s.workqueue import queue


def new():
    return _DelayingType(clock.RealClock())


class _DelayingType(queue.Type):
    def __init__(self, c):
        super().__init__(c)
        self._heartbeat = c.new_ticker(_MAX_WAIT)
        self._stop_event = asyncio.Event()
        self._waiting_for_add_queue = asyncio.Queue(maxsize=1000)
        asyncio.ensure_future(self._waiting_loop())

    async def add_after(self, item, duration):
        if self.shutting_down():
            return

        if duration <= 0:
            await self.add(item)
            return

        _, pending = await asyncio.wait(
            [
                asyncio.ensure_future(self._stop_event.wait()),
                asyncio.ensure_future(
                    self._waiting_for_add_queue.put(
                        _WaitFor(data=item, ready_at=self._clock.now() + duration)
                    )
                ),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def shut_down(self):
        if not self._stop_event.is_set():
            self._stop_event.set()
            await self.shut_down()
            self._heartbeat.stop()

    async def _waiting_loop(self):
        next_ready_at_timer = None
        waiting_for_queue = _WaitForPriorityQueue()

        while not self.shutting_down():
            now = self._clock.now()

            while len(waiting_for_queue):
                entry_ready_at = waiting_for_queue.peek_priority()
                if entry_ready_at > now:
                    break

                entry_data = waiting_for_queue.pop()
                await self.add(entry_data)

            next_ready_at = asyncio.Queue()
            if len(waiting_for_queue):
                if next_ready_at_timer:
                    next_ready_at_timer.stop()
                entry_ready_at = waiting_for_queue.peek_priority()
                next_ready_at_timer = self._clock.new_timer(entry_ready_at - now)
                next_ready_at = next_ready_at_timer.c()

            wait_task = asyncio.ensure_future(self._waiting_for_add_queue.get())
            done, pending = await asyncio.wait(
                [
                    asyncio.ensure_future(self._stop_event.wait()),
                    asyncio.ensure_future(self._heartbeat.c().get()),
                    asyncio.ensure_future(next_ready_at.get()),
                    wait_task,
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if self._stop_event.is_set():
                return
            if wait_task in done:
                wait_entry = await wait_task
                if wait_entry.ready_at > self._clock.now():
                    waiting_for_queue.insert(wait_entry.data, wait_entry.ready_at)
                else:
                    await self.add(wait_entry.data)

                while not self._waiting_for_add_queue.empty():
                    wait_entry = self._waiting_for_add_queue.get_nowait()
                    if wait_entry.ready_at > self._clock.now():
                        waiting_for_queue.insert(wait_entry.data, wait_entry.ready_at)
                    else:
                        await self.add(wait_entry.data)


class _WaitFor(NamedTuple):
    data: Hashable
    ready_at: float


class _WaitForPriorityQueue:
    _REMOVED = object()

    def __init__(self):
        self._pq = []
        self._entry_finder = {}
        self._counter = itertools.count()
        self._len = 0

    def __len__(self):
        return self._len

    def insert(self, data, priority):
        if data in self._entry_finder:
            if not self._remove(data, priority):
                return
        else:
            self._len += 1
        count = next(self._counter)
        entry = [priority, count, data]
        self._entry_finder[data] = entry
        heapq.heappush(self._pq, entry)

    def peek_priority(self):
        while self._pq:
            priority, _, data = self._pq[0]
            if data is not self._REMOVED:
                return priority
            heapq.heappop(self._pq)
        raise KeyError

    def pop(self):
        while self._pq:
            _, _, data = heapq.heappop(self._pq)
            if data is not self._REMOVED:
                del self._entry_finder[data]
                self._len -= 1
                return data
        raise KeyError

    def _remove(self, data, priority):
        entry = self._entry_finder[data]
        if entry[0] > priority:
            self._entry_finder.pop(data)
            entry[-1] = self._REMOVED
            return True
        return False


_MAX_WAIT = 10
