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

import copy
import unittest

from kubernetes.client.models import V1ObjectMeta, V1Pod

from aiok8s.cache import store
from aiok8s.cache.index import Indexers
from aiok8s.cache.testing.util import async_test


class TestIndex(unittest.TestCase):
    @async_test
    async def test_get_index_func_values(self):
        index = store.new_indexer(
            store.meta_namespace_key_func, Indexers(testmodes=test_index_func)
        )

        pod1 = V1Pod(metadata=V1ObjectMeta(name="one", labels={"foo": "bar"}))
        pod2 = V1Pod(metadata=V1ObjectMeta(name="two", labels={"foo": "bar"}))
        pod3 = V1Pod(metadata=V1ObjectMeta(name="tre", labels={"foo": "biz"}))

        await index.add(pod1)
        await index.add(pod2)
        await index.add(pod3)

        keys = await index.list_index_func_values("testmodes")
        self.assertEqual(len(keys), 2)

        for key in keys:
            self.assertIn(key, ("bar", "biz"))

    @async_test
    async def test_multi_index_keys(self):
        index = store.new_indexer(
            store.meta_namespace_key_func, Indexers(by_user=test_users_index_func)
        )

        pod1 = V1Pod(
            metadata=V1ObjectMeta(name="one", annotations={"users": "ernie,bert"})
        )
        pod2 = V1Pod(
            metadata=V1ObjectMeta(name="two", annotations={"users": "bert,oscar"})
        )
        pod3 = V1Pod(
            metadata=V1ObjectMeta(name="tre", annotations={"users": "ernie,elmo"})
        )

        await index.add(pod1)
        await index.add(pod2)
        await index.add(pod3)

        expected = {
            "ernie": {"one", "tre"},
            "bert": {"one", "two"},
            "elmo": {"tre"},
            "oscar": {"two"},
        }
        for k, v in expected.items():
            index_results = await index.by_index("by_user", k)
            found = {item.metadata.name for item in index_results}
            self.assertEqual(found, v)

        await index.delete(pod3)
        ernie_pods = await index.by_index("by_user", "ernie")
        self.assertEqual(len(ernie_pods), 1)
        for ernie_pod in ernie_pods:
            self.assertEqual(ernie_pod.metadata.name, "one")

        elmo_pods = await index.by_index("by_user", "elmo")
        self.assertEqual(len(elmo_pods), 0)

        copy_of_pod2 = copy.deepcopy(pod2)
        copy_of_pod2.metadata.annotations["users"] = "oscar"
        await index.update(copy_of_pod2)
        bert_pods = await index.by_index("by_user", "bert")
        self.assertEqual(len(bert_pods), 1)
        for bert_pod in bert_pods:
            self.assertEqual(bert_pod.metadata.name, "one")


def test_index_func(obj):
    return [obj.metadata.labels["foo"]]


def test_users_index_func(obj):
    users_string = obj.metadata.annotations["users"]
    return users_string.split(",")


if __name__ == "__main__":
    unittest.main()
