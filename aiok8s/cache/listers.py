# Copyright 2014 The Kubernetes Authors.
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

def new_generic_lister(indexer):
    return _GenericLister(indexer)


class _GenericLister:
    def __init__(self, indexer):
        self._indexer = indexer

    def get(self, name):
        obj = self._indexer.get_by_key(name)
        if obj is None:
            raise KeyError
        return obj

    def by_namespace(self, namespace):
        return _GenericNamespaceLister(self._indexer, namespace)


class _GenericNamespaceLister:
    def __init__(self, indexer, namespace):
        self._indexer = indexer
        self._namespace = namespace

    def get(self, name):
        obj = self._indexer.get_by_key(f"{self._namespace}/{name}")
        if obj is None:
            raise KeyError
        return obj
