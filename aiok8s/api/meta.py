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


def accessor(obj):
    return _Accessor(obj)


def list_accessor(obj):
    return _ListAccessor(obj)


def extract_list(obj):
    return obj.items


def set_list(list_, objects):
    list_.items = objects


class _Accessor:
    def __init__(self, obj):
        self._obj = obj

    @property
    def namespace(self):
        return self._obj.metadata.namespace

    @property
    def name(self):
        return self._obj.metadata.name

    @property
    def uid(self):
        return self._obj.metadata.uid

    @property
    def resource_version(self):
        return self._obj.metadata.resource_version

    @resource_version.setter
    def resource_version(self, resource_version):
        self._obj.metadata.resource_version = resource_version


class _ListAccessor:
    def __init__(self, obj):
        self._obj = obj

    @property
    def resource_version(self):
        return self._obj.metadata.resource_version

    @resource_version.setter
    def resource_version(self, resource_version):
        self._obj.metadata.resource_version = resource_version
