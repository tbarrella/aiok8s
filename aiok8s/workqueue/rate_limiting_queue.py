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

from aiok8s.util import clock
from aiok8s.workqueue import default_rate_limiters, delaying_queue


def new(rate_limiter=None):
    if not rate_limiter:
        rate_limiter = default_rate_limiters.default_controller_rate_limiter()
    return _RateLimitingType(rate_limiter)


class _RateLimitingType(delaying_queue._DelayingType):
    def __init__(self, rate_limiter, c=None):
        if not c:
            c = clock.RealClock()
        super().__init__(c)
        self._rate_limiter = rate_limiter

    async def add_rate_limited(self, item):
        await self.add_after(item, self._rate_limiter.when(item))

    def forget(self, item):
        self._rate_limiter.forget(item)

    def num_requeues(self, item):
        return self._rate_limiter.num_requeues(item)
