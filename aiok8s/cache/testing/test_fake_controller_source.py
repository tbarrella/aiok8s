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
import unittest

from kubernetes.client.models import V1ObjectMeta, V1Pod

from aiok8s.cache.testing.fake_controller_source import FakeControllerSource
from aiok8s.cache.testing.util import async_test


class TestFakeControllerSource(unittest.TestCase):
    @async_test
    async def test_rc_number(self):
        def pod(name):
            return V1Pod(metadata=V1ObjectMeta(name=name))

        queue = asyncio.Queue(maxsize=3)
        for _ in range(3):
            await queue.put(None)

        source = FakeControllerSource()
        await source.add(pod("foo"))
        await source.modify(pod("foo"))
        await source.modify(pod("foo"))

        w = await source.watch({"resource_version": "1"})
        asyncio.ensure_future(self.consume(w, ["2", "3"], queue))

        list_ = await source.list({})
        self.assertEqual(list_.metadata.resource_version, "3")

        w2 = await source.watch({"resource_version": "2"})
        asyncio.ensure_future(self.consume(w2, ["3"], queue))

        w3 = await source.watch({"resource_version": "3"})
        asyncio.ensure_future(self.consume(w3, [], queue))
        await source.shutdown()
        await queue.join()

    async def consume(self, w, rvs, done):
        try:
            i = 0
            async for got in w:
                rv = rvs[i]
                got_rv = got["object"].metadata.resource_version
                self.assertEqual(got_rv, rv)
                i += 1
            self.assertEqual(i, len(rvs))
        finally:
            done.task_done()


if __name__ == "__main__":
    unittest.main()
