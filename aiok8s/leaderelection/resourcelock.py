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

import json
from typing import NamedTuple

from kubernetes_asyncio.client import apis, models

LEADER_ELECTION_RECORD_ANNOTATION_KEY = "control-plane.alpha.kubernetes.io/leader"


class ResourceLockConfig(NamedTuple):
    identity: str


def new(namespace, name, rlc, client=None):
    return ConfigMapLock(
        config_map_meta=models.V1ObjectMeta(namespace=namespace, name=name),
        client=apis.CoreV1Api(client),
        lock_config=rlc,
    )


class ConfigMapLock:
    def __init__(self, config_map_meta, client, lock_config):
        self.config_map_meta = config_map_meta
        self.client = client
        self.lock_config = lock_config
        self._cm = None

    async def get(self):
        self._cm = await self.client.read_namespaced_config_map(
            self.config_map_meta.name, self.config_map_meta.namespace
        )
        raw_record = (
            self._cm.metadata.annotations
            and self._cm.metadata.annotations.get(LEADER_ELECTION_RECORD_ANNOTATION_KEY)
        )
        record = raw_record and json.loads(raw_record)
        return record, raw_record

    async def create(self, leader_election_record):
        raw_record = json.dumps(leader_election_record)
        self._cm = await self.client.create_namespaced_config_map(
            self.config_map_meta.namespace,
            models.V1ConfigMap(
                metadata=models.V1ObjectMeta(
                    name=self.config_map_meta.name,
                    namespace=self.config_map_meta.namespace,
                    annotations={LEADER_ELECTION_RECORD_ANNOTATION_KEY: raw_record},
                )
            ),
        )

    async def update(self, leader_election_record):
        if not self._cm:
            raise Exception("configmap not initialized, call get or create first")
        raw_record = json.dumps(leader_election_record)
        self._cm.metadata.annotations[
            LEADER_ELECTION_RECORD_ANNOTATION_KEY
        ] = raw_record
        self._cm = await self.client.replace_namespaced_config_map(
            self.config_map_meta.name, self.config_map_meta.namespace, self._cm
        )

    def describe(self):
        return f"{self.config_map_meta.namespace}/{self.config_map_meta.name}"

    def identity(self):
        return self.lock_config.identity
