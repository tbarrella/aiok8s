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

from aiok8s.controller import reconcile

logger = logging.getLogger(__name__)


class EnqueueRequestForObject:
    async def create(self, event, queue):
        if not event.meta:
            logger.error("CreateEvent received with no metadata")
            return
        await queue.add(
            reconcile.Request(
                reconcile.NamespacedName(
                    name=event.meta.name, namespace=event.meta.namespace
                )
            )
        )

    async def update(self, event, queue):
        if event.meta_old:
            await queue.add(
                reconcile.Request(
                    reconcile.NamespacedName(
                        name=event.meta_old.name, namespace=event.meta_old.namespace
                    )
                )
            )
        else:
            logger.error("UpdateEvent received with no old metadata")

        if event.meta_new:
            await queue.add(
                reconcile.Request(
                    reconcile.NamespacedName(
                        name=event.meta_new.name, namespace=event.meta_new.namespace
                    )
                )
            )
        else:
            logger.error("UpdateEvent received with no new metadata")

    async def delete(self, event, queue):
        if not event.meta:
            logger.error("DeleteEvent received with no metadata")
            return
        await queue.add(
            reconcile.Request(
                reconcile.NamespacedName(
                    name=event.meta.name, namespace=event.meta.namespace
                )
            )
        )
