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

"""

Functions for accessing object data.

This departs from the Go implementation because objects returned from the Python API
don't satisfy a common interface. Some are typed objects and some are raw dicts. The
functions here wrap an object with accessors that depend on the object's type.

"""

from aiok8s.runtime import schema


def list_accessor(obj):
    try:
        if isinstance(obj, dict):
            return _UnstructuredListAccessor(obj)
        return _ListAccessor(obj)
    except (AttributeError, KeyError):
        raise _NotListError


def accessor(obj):
    try:
        if isinstance(obj, dict):
            return _UnstructuredAccessor(obj)
        return _Accessor(obj)
    except (AttributeError, KeyError):
        raise _NotObjectError


def type_accessor(obj):
    if isinstance(obj, dict):
        return _UnstructuredObjectAccessor(obj)
    return _ObjectAccessor(obj)


def extract_list(obj):
    if isinstance(obj, dict):
        return obj["items"]
    return obj.items


def set_list(list_, objects):
    if isinstance(list_, dict):
        list_["items"] = objects
    else:
        list_.items = objects


class _NotListError(Exception):
    pass


class _NotObjectError(Exception):
    pass


class _ListAccessor:
    def __init__(self, obj):
        self._metadata = obj.metadata

    @property
    def resource_version(self):
        return self._metadata.resource_version

    @resource_version.setter
    def resource_version(self, resource_version):
        self._metadata.resource_version = resource_version


class _UnstructuredListAccessor:
    def __init__(self, obj):
        self._metadata = obj["metadata"]

    @property
    def resource_version(self):
        return self._metadata["resourceVersion"]

    @resource_version.setter
    def resource_version(self, resource_version):
        self._metadata["resourceVersion"] = resource_version


class _Accessor:
    def __init__(self, obj):
        self._metadata = obj.metadata

    @property
    def namespace(self):
        return self._metadata.namespace

    @property
    def name(self):
        return self._metadata.name

    @property
    def uid(self):
        return self._metadata.uid

    @property
    def resource_version(self):
        return self._metadata.resource_version

    @resource_version.setter
    def resource_version(self, resource_version):
        self._metadata.resource_version = resource_version


class _UnstructuredAccessor:
    def __init__(self, obj):
        self._metadata = obj["metadata"]

    @property
    def namespace(self):
        return self._metadata["namespace"]

    @property
    def name(self):
        return self._metadata["name"]

    @property
    def uid(self):
        return self._metadata["uid"]

    @property
    def resource_version(self):
        return self._metadata["resourceVersion"]

    @resource_version.setter
    def resource_version(self, resource_version):
        self._metadata["resourceVersion"] = resource_version


class _ObjectAccessor:
    def __init__(self, obj):
        self._obj = obj

    @property
    def api_version(self):
        return self._obj.api_version

    @property
    def kind(self):
        return self._obj.kind

    # Motivated by apimachinery/pkg/apis/meta/v1/meta.go GroupVersionKind()
    @property
    def group_version_kind(self):
        group, version = self.api_version.split("/")
        return schema.GroupVersionKind(group, version, self.kind)


class _UnstructuredObjectAccessor(_ObjectAccessor):
    @property
    def api_version(self):
        return self._obj["apiVersion"]

    @property
    def kind(self):
        return self._obj["kind"]
