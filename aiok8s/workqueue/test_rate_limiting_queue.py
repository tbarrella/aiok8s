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

import time
import unittest.mock

from aiok8s.cache.testing.util import async_test
from aiok8s.util import clock
from aiok8s.workqueue import default_rate_limiters, delaying_queue
from aiok8s.workqueue.rate_limiting_queue import _RateLimitingType


class TestRateLimitingQueue(unittest.TestCase):
    @async_test
    async def test(self):
        limiter = default_rate_limiters.ItemExponentialFailureRateLimiter(0.001, 1)
        fake_clock = clock.FakeClock(time.time())
        with unittest.mock.patch.object(
            delaying_queue._DelayingType, "_waiting_loop", _null_loop
        ):
            queue = _RateLimitingType(limiter, fake_clock)

        await queue.add_rate_limited("one")
        wait_entry = await queue._waiting_for_add_queue.get()
        self.assertAlmostEqual(wait_entry.ready_at - fake_clock.now(), 0.001, places=6)
        await queue.add_rate_limited("one")
        wait_entry = await queue._waiting_for_add_queue.get()
        self.assertAlmostEqual(wait_entry.ready_at - fake_clock.now(), 0.002, places=6)
        self.assertEqual(queue.num_requeues("one"), 2)

        await queue.add_rate_limited("two")
        wait_entry = await queue._waiting_for_add_queue.get()
        self.assertAlmostEqual(wait_entry.ready_at - fake_clock.now(), 0.001, places=6)
        await queue.add_rate_limited("two")
        wait_entry = await queue._waiting_for_add_queue.get()
        self.assertAlmostEqual(wait_entry.ready_at - fake_clock.now(), 0.002, places=6)

        queue.forget("one")
        self.assertEqual(queue.num_requeues("one"), 0)
        await queue.add_rate_limited("one")
        wait_entry = await queue._waiting_for_add_queue.get()
        self.assertAlmostEqual(wait_entry.ready_at - fake_clock.now(), 0.001, places=6)


async def _null_loop(_):
    pass


if __name__ == "__main__":
    unittest.main()
