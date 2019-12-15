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
import unittest

from aiok8s.workqueue.default_rate_limiters import (
    BucketRateLimiter,
    ItemExponentialFailureRateLimiter,
)


class TestDefaultRateLimiters(unittest.TestCase):
    def test_bucket_rate_limiter(self):
        t0 = time.time() * 1_000_000_000
        t3 = t0 + 300_000_000
        limiter = BucketRateLimiter(10, 2)

        tokens = limiter._advance(t0)
        self.assertEqual(tokens, 2)
        limiter._last = t0
        limiter._tokens = 1

        tokens = limiter._advance(t0)
        self.assertEqual(tokens, 1)
        limiter._tokens = 0

        tokens = limiter._advance(t0)
        self.assertEqual(tokens, 0)
        limiter._tokens = -1

        tokens = limiter._advance(t0)
        self.assertEqual(tokens, -1)
        limiter._tokens = -2

        tokens = limiter._advance(t3)
        self.assertEqual(tokens, 1)
        limiter._last = t3
        limiter._tokens = 0

        tokens = limiter._advance(t3)
        self.assertEqual(tokens, 0)

    def test_item_exponential_failure_rate_limiter(self):
        limiter = ItemExponentialFailureRateLimiter(0.001, 1)
        self.assertEqual(limiter.when("one"), 0.001)
        self.assertEqual(limiter.when("one"), 0.002)
        self.assertEqual(limiter.when("one"), 0.004)
        self.assertEqual(limiter.when("one"), 0.008)
        self.assertEqual(limiter.when("one"), 0.016)
        self.assertEqual(limiter.num_requeues("one"), 5)

        self.assertEqual(limiter.when("two"), 0.001)
        self.assertEqual(limiter.when("two"), 0.002)
        self.assertEqual(limiter.num_requeues("two"), 2)

        limiter.forget("one")
        self.assertEqual(limiter.num_requeues("one"), 0)
        self.assertEqual(limiter.when("one"), 0.001)

    def test_item_exponential_failure_rate_limiter_overflow(self):
        limiter = ItemExponentialFailureRateLimiter(0.001, 1000)
        for _ in range(5):
            limiter.when("one")
        self.assertEqual(limiter.when("one"), 0.032)

        for _ in range(1000):
            limiter.when("overflow1")
        self.assertEqual(limiter.when("overflow1"), 1000)

        limiter = ItemExponentialFailureRateLimiter(60, 1000 * 3600)
        for _ in range(2):
            limiter.when("two")
        self.assertEqual(limiter.when("two"), 4 * 60)

        for _ in range(1000):
            limiter.when("overflow2")
        self.assertEqual(limiter.when("overflow2"), 1000 * 3600)


if __name__ == "__main__":
    unittest.main()
