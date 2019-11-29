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
import random
import string
import unittest
from collections import defaultdict

from kubernetes.client.models import V1ObjectMeta, V1Pod

from aiok8s.util import wait
from aiok8s.cache.controller import (
    ResourceEventHandlerFuncs,
    deletion_handling_meta_namespace_key_func,
    new_informer,
)
from aiok8s.cache.testing import fake_controller_source
from aiok8s.cache.testing.util import async_test


class TestController(unittest.TestCase):
    @async_test
    async def test_hammer(self):
        source = fake_controller_source.FakeControllerSource()
        output_set_lock = asyncio.Lock()
        output_set = defaultdict(list)

        async def record_func(event_type, obj):
            key = deletion_handling_meta_namespace_key_func(obj)
            async with output_set_lock:
                output_set[key].append(event_type)

        async def add_func(obj):
            await record_func("add", obj)

        async def update_func(old_obj, new_obj):
            await record_func("update", new_obj)

        async def delete_func(obj):
            await record_func("delete", obj)

        h = ResourceEventHandlerFuncs(
            add_func=add_func, update_func=update_func, delete_func=delete_func
        )
        _, controller = new_informer(source, V1Pod(), 0.1, h)
        self.assertFalse(controller.has_synced())

        stop = asyncio.Event()
        asyncio.ensure_future(controller.run(stop))

        await wait.poll(0.1, wait.FOREVER_TEST_TIMEOUT, controller.has_synced)
        self.assertTrue(controller.has_synced())

        async def task():
            current_names = set()
            for _ in range(100):
                if not current_names or not random.randrange(3):
                    name = "".join(
                        random.choice(string.ascii_letters) for _ in range(16)
                    )
                    is_new = True
                else:
                    name = random.choice(list(current_names))
                    is_new = False
                # TODO: fuzz
                pod = V1Pod(metadata=V1ObjectMeta(name=name, namespace="default"))
                if is_new:
                    current_names.add(name)
                    await source.add(pod)
                    continue
                if random.randrange(2):
                    current_names.add(name)
                    await source.modify(pod)
                else:
                    current_names.remove(name)
                    await source.delete(pod)

        await asyncio.gather(*(task() for _ in range(3)))

        await asyncio.sleep(0.1)
        stop.set()
        # TODO: Figure out why this is necessary...
        await asyncio.sleep(0.1)

        await output_set_lock.acquire()

    @async_test
    async def test_update(self):
        source = fake_controller_source.FakeControllerSource()
        _FROM = "from"
        _TO = "to"
        allowed_transitions = {(_FROM, _TO), (_TO, _TO), (_FROM, _FROM)}

        def pod(name, check, final):
            labels = {"check": check}
            if final:
                labels["final"] = "true"
            return V1Pod(metadata=V1ObjectMeta(name=name, labels=labels))

        def delete_pod(p):
            return p.metadata.labels["final"] == "true"

        async def test(name):
            name = f"a-{name}"
            await source.add(pod(name, _FROM, False))
            await source.modify(pod(name, _TO, True))

        tests = [test]
        threads = 3
        size = threads * len(tests)
        test_done_queue = asyncio.Queue(maxsize=size)
        for _ in range(size):
            test_done_queue.put_nowait(None)

        watch_event = asyncio.Event()

        async def watch_func(options):
            try:
                return await source.watch(options)
            finally:
                watch_event.set()

        async def update_func(old_obj, new_obj):
            from_ = old_obj.metadata.labels["check"]
            to = new_obj.metadata.labels["check"]
            self.assertIn((from_, to), allowed_transitions)
            if delete_pod(new_obj):
                await source.delete(new_obj)

        async def delete_func(obj):
            test_done_queue.task_done()

        lw = TestLW(source.list, watch_func)
        h = ResourceEventHandlerFuncs(update_func=update_func, delete_func=delete_func)
        _, controller = new_informer(lw, V1Pod(), 0, h)

        stop = asyncio.Event()
        asyncio.ensure_future(controller.run(stop))
        await watch_event.wait()

        aws = [f(f"{i}-{j}") for i in range(threads) for j, f in enumerate(tests)]
        await asyncio.gather(*aws)
        await test_done_queue.join()
        stop.set()

        # TODO: Figure out why this is necessary...
        await asyncio.sleep(0.1)


class TestLW:
    def __init__(self, list_func, watch_func):
        self._list_func = list_func
        self._watch_func = watch_func

    async def list(self, options):
        return await self._list_func(options)

    async def watch(self, options):
        return await self._watch_func(options)


if __name__ == "__main__":
    unittest.main()
