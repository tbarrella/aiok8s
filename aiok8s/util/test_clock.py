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

import time
import unittest

from aiok8s.cache.testing.util import async_test
from aiok8s.util.clock import FakeClock, FakePassiveClock


class TestClock(unittest.TestCase):
    @async_test
    async def test_fake_passive_clock(self):
        start_time = time.time()
        tc = FakePassiveClock(start_time)
        await self.exercise_passive_clock(tc)

    @async_test
    async def test_fake_clock(self):
        start_time = time.time()
        tc = FakeClock(start_time)
        await self.exercise_passive_clock(tc)
        await tc.set_time(start_time)
        await tc.step(1)
        now = tc.now()
        self.assertEqual(now - start_time, 1)

    @async_test
    async def test_fake_timer(self):
        tc = FakeClock(time.time())
        self.assertFalse(tc.has_waiters())
        one_sec = tc.new_timer(1)
        two_sec = tc.new_timer(2)
        tre_sec = tc.new_timer(3)
        self.assertTrue(tc.has_waiters())
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        await tc.step(0.999999)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        await tc.step(0.000001)
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        one_sec.c().get_nowait()
        await tc.step(0.000001)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        self.assertFalse(one_sec.stop())
        self.assertTrue(two_sec.stop())
        await tc.step(1)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        self.assertFalse(two_sec.reset(1))
        self.assertTrue(tre_sec.reset(1))
        await tc.step(0.999999)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        self.assertTrue(tre_sec.c().empty())
        await tc.step(0.000001)
        self.assertTrue(one_sec.c().empty())
        self.assertTrue(two_sec.c().empty())
        tre_sec.c().get_nowait()

    async def exercise_passive_clock(self, pc):
        t1 = time.time()
        t2 = t1 + 3600
        await pc.set_time(t1)
        tx = pc.now()
        self.assertEqual(tx, t1)
        dx = pc.since(t1)
        self.assertEqual(dx, 0)
        await pc.set_time(t2)
        dx = pc.since(t1)
        self.assertEqual(dx, 3600)
        tx = pc.now()
        self.assertEqual(tx, t2)


if __name__ == "__main__":
    unittest.main()
