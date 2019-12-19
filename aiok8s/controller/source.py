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

import logging
from typing import Any, Collection, NamedTuple

from aiok8s.api import meta
from aiok8s.controller import event

logger = logging.getLogger(__name__)


class Kind:
    def __init__(self, type_):
        self.type = type_
        self._cache = None

    async def start(self, handler, queue, *predicates):
        if not self._cache:
            raise Exception("must call inject_cache on Kind before calling start")
        informer = await self._cache.get_informer(self.type)
        await informer.add_event_handler(EventHandler(handler, queue, predicates))

    def inject_cache(self, c):
        if not self._cache:
            self._cache = c


class EventHandler(NamedTuple):
    event_handler: Any  # handler.EventHandler
    queue: Any  # RateLimitingInterface
    predicates: Collection[Any]  # predicate.Predicate

    async def on_add(self, obj):
        try:
            metadata = meta.accessor(obj)
        except Exception:
            logger.error("on_add missing meta")
            return

        create_event = event.CreateEvent(metadata, obj)
        for p in self.predicates:
            if not p.create(create_event):
                return

        await self.event_handler.create(create_event, self.queue)

    async def on_update(self, old_obj, new_obj):
        try:
            meta_old = meta.accessor(old_obj)
        except Exception:
            logger.error("on_update missing meta_old")
            return

        try:
            meta_new = meta.accessor(new_obj)
        except Exception:
            logger.error("on_update missing meta_new")
            return

        update_event = event.UpdateEvent(
            meta_old=meta_old, object_old=old_obj, meta_new=meta_new, object_new=new_obj
        )
        for p in self.predicates:
            if not p.update(update_event):
                return

        await self.event_handler.update(update_event, self.queue)

    async def on_delete(self, obj):
        try:
            metadata = meta.accessor(obj)
        except Exception:
            logger.error("on_delete missing meta")
            return

        delete_event = event.DeleteEvent(metadata, obj)
        for p in self.predicates:
            if not p.delete(delete_event):
                return

        await self.event_handler.delete(delete_event, self.queue)
