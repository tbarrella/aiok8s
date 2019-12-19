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

from aiok8s.controller import controller as _controller
from aiok8s.controller import handler as _handler
from aiok8s.controller import source as _source


async def build_controller(manager, reconciler, *, api_type):
    controller = _do_controller(manager, reconciler)

    # NB: Different from `controller-runtime`
    # Create informer before starting the cache to simplify logic
    await manager.get_cache().get_informer(api_type)

    await _do_watch(controller, api_type)


async def _do_watch(controller, api_type):
    source = _source.Kind(api_type)
    handler = _handler.EnqueueRequestForObject()
    await controller.watch(source, handler)


def _do_controller(manager, reconciler):
    return _controller.new(manager, reconciler=reconciler)
