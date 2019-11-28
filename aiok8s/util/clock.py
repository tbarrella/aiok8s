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
import time

from aiok8s.util import _time


class RealClock:
    def now(self):
        return time.time()

    def since(self, ts):
        return time.time() - ts

    def new_timer(self, d):
        return _RealTimer(_time.Timer(d))


class FakePassiveClock:
    def __init__(self, t):
        self._lock = asyncio.Lock()
        self._time = t

    def now(self):
        return self._time

    def since(self, ts):
        return self._time - ts

    async def set_time(self, t):
        self._time = t


class FakeClock(FakePassiveClock):
    def __init__(self, t):
        super().__init__(t)
        self._waiters = []

    def new_timer(self, d):
        stop_time = self._time + d
        queue = asyncio.Queue(maxsize=1)
        waiter = _FakeClockWaiter(target_time=stop_time, dest_queue=queue)
        self._waiters.append(waiter)
        return _FakeTimer(fake_clock=self, waiter=waiter)

    async def step(self, d):
        async with self._lock:
            await self._set_time_locked(self._time + d)

    async def set_time(self, t):
        async with self._lock:
            await self._set_time_locked(t)

    def has_waiters(self):
        return bool(self._waiters)

    async def _set_time_locked(self, t):
        self._time = t
        new_waiters = []
        for w in self._waiters:
            if w._target_time <= t:
                if w._skip_if_blocked:
                    if not self._dest_queue.full():
                        w._dest_queue.put_nowait(t)
                else:
                    await w._dest_queue.put(t)

                if w._step_interval:
                    while w._target_time <= t:
                        w._target_time += w._step_interval
                    new_waiters.append(w)
            else:
                new_waiters.append(w)
        self._waiters = new_waiters


class _FakeClockWaiter:
    def __init__(
        self, *, target_time, dest_queue, step_interval=0, skip_if_blocked=False
    ):
        self._target_time = target_time
        self._dest_queue = dest_queue
        self._step_interval = step_interval
        self._skip_if_blocked = skip_if_blocked


class _RealTimer:
    def __init__(self, timer):
        self._timer = timer

    def c(self):
        return self._timer.c

    def stop(self):
        return self._timer.stop()


class _FakeTimer:
    def __init__(self, *, fake_clock, waiter):
        self._fake_clock = fake_clock
        self._waiter = waiter

    def c(self):
        return self._waiter._dest_queue

    def stop(self):
        stopped = False
        old_waiters = self._fake_clock._waiters
        new_waiters = []
        seek_queue = self._waiter._dest_queue
        for w in old_waiters:
            if w._dest_queue is seek_queue:
                stopped = True
            else:
                new_waiters.append(w)
        self._fake_clock._waiters = new_waiters
        return stopped

    def reset(self, d):
        waiters = self._fake_clock._waiters
        seek_queue = self._waiter._dest_queue
        for w in waiters:
            if w._dest_queue is seek_queue:
                w._target_time = self._fake_clock._time + d
                return True
        return False
