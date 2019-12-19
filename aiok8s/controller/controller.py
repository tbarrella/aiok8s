# Copyright 2018 The Kubernetes Authors.
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
import logging
from typing import Any, Collection, NamedTuple

from aiok8s.controller import reconcile
from aiok8s.workqueue import rate_limiting_queue

logger = logging.getLogger(__name__)


def new(manager, *, reconciler):
    manager.set_fields(reconciler)
    controller = _Controller(
        do=reconciler, cache=manager.get_cache(), make_queue=rate_limiting_queue.new
    )
    manager.add(controller)
    return controller


class _Controller:
    def __init__(self, do, cache, make_queue):
        self.do = do
        self.cache = cache
        self.make_queue = make_queue
        self.max_concurrent_reconciles = 1
        self.jitter_period = 1
        self._mutex = asyncio.Lock()
        self._watches = []

    async def reconcile(self, request):
        return await self.do.reconcile(request)

    async def watch(self, source, event_handler, *predicates):
        async with self._mutex:
            self.set_fields(source)
            self.set_fields(event_handler)
            for predicate in predicates:
                self.set_fields(predicate)

            self._watches.append(_WatchDescription(source, event_handler, predicates))

    async def start(self):
        self.queue = self.make_queue()
        try:
            async with self._mutex:
                for watch in self._watches:
                    await watch.source.start(
                        watch.handler, self.queue, *watch.predicates
                    )

                if not await self.cache.wait_for_cache_sync():
                    raise Exception("failed to wait for caches to sync")

                async def aw():
                    while True:
                        await self._worker()
                        await asyncio.sleep(self.jitter_period)

                aws = [
                    asyncio.ensure_future(aw())
                    for _ in range(self.max_concurrent_reconciles)
                ]

            await asyncio.gather(*aws)
        finally:
            await self.queue.shut_down()

    def inject_func(self, f):
        self.set_fields = f

    async def _worker(self):
        while await self._process_next_work_item():
            pass

    async def _process_next_work_item(self):
        obj, shutdown = await self.queue.get()
        if shutdown:
            return False
        try:
            return await self._reconcile_handler(obj)
        except Exception:
            logger.exception("")
        finally:
            await self.queue.done(obj)

    async def _reconcile_handler(self, obj):
        if not isinstance(obj, reconcile.Request):
            self.queue.forget(obj)
            logger.error("queue item was not a Request")
            return True

        try:
            result = await self.do.reconcile(obj)
        except asyncio.CancelledError:
            raise
        except Exception:
            await self.queue.add_rate_limited(obj)
            logger.exception("Reconciler error")
            return False
        if result.requeue_after:
            self.queue.forget(obj)
            await self.queue.add_after(obj, result.requeue_after)
            return True
        if result.requeue:
            await self.queue.add_rate_limited(obj)
            return True

        self.queue.forget(obj)

        logger.info("Successfully reconciled")
        return True


class _WatchDescription(NamedTuple):
    source: Any  # source.Source
    handler: Any  # handler.EventHandler
    predicates: Collection[Any]  # predicate.Predicate
