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


class CacheReader:
    def __init__(self, indexer, group_version_kind):
        self._indexer = indexer
        self._group_version_kind = group_version_kind

    def get(self, key):
        store_key = _object_key_to_store_key(key)
        obj = self._indexer.get_by_key(store_key)
        if obj is None:
            raise KeyError
        return obj


def _object_key_to_store_key(key):
    if not key.namespace:
        return key.name
    return f"{key.namespace}/{key.name}"
