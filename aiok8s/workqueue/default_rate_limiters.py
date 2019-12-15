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

import collections
import operator
import time


def default_controller_rate_limiter():
    return _MaxOfRateLimiters(
        ItemExponentialFailureRateLimiter(0.005, 1000), BucketRateLimiter(10, 100)
    )


class BucketRateLimiter:
    def __init__(self, limit, burst):
        if limit <= 0:
            raise ValueError
        if burst < 1:
            raise ValueError
        self._limit = limit
        self._burst = burst
        self._tokens = 0
        self._last = 0

    def when(self, item):
        now = time.time() * 1_000_000_000
        tokens = self._advance(now) - 1
        wait_duration = -tokens / self._limit if tokens < 0 else 0
        self._last = now
        self._tokens = tokens
        return wait_duration

    def forget(self, item):
        pass

    def num_requeues(self, item):
        return 0

    def _advance(self, now):
        last = min(self._last, now)
        max_elapsed = 1_000_000_000 * (self._burst - self._tokens) / self._limit
        elapsed = min(now - last, max_elapsed)
        delta = elapsed * self._limit / 1_000_000_000
        tokens = min(self._tokens + delta, self._burst)
        return tokens


class ItemExponentialFailureRateLimiter:
    def __init__(self, base_delay, max_delay):
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._failures = collections.Counter()

    def when(self, item):
        exp = self._failures[item]
        self._failures[item] += 1

        backoff = self._base_delay * 2 ** exp
        calculated = min(backoff, self._max_delay)
        return calculated

    def forget(self, item):
        del self._failures[item]

    def num_requeues(self, item):
        return self._failures[item]


class _MaxOfRateLimiters:
    def __init__(self, *limiters):
        self._limiters = limiters

    def when(self, item):
        return max(map(operator.methodcaller("when", item), self._limiters))

    def forget(self, item):
        for limiter in self._limiters:
            limiter.forget(item)

    def num_requeues(self, item):
        return max(map(operator.methodcaller("num_requeues", item), self._limiters))
