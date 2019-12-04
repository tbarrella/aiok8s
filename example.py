"""

Example using a pod informer.

Usage:
kind create cluster
python example.py

Output:
added kube-proxy-c7d7p
added kindnet-t5bmr
added coredns-5c98db65d4-qdv6t
added kube-apiserver-kind-control-plane
added kube-scheduler-kind-control-plane
added etcd-kind-control-plane
added coredns-5c98db65d4-br9qg
added kube-controller-manager-kind-control-plane
updated coredns-5c98db65d4-br9qg
updated coredns-5c98db65d4-br9qg
added coredns-5c98db65d4-7wtns
updated coredns-5c98db65d4-7wtns
updated coredns-5c98db65d4-7wtns
updated coredns-5c98db65d4-7wtns
updated coredns-5c98db65d4-br9qg
updated coredns-5c98db65d4-br9qg
updated coredns-5c98db65d4-br9qg
deleted coredns-5c98db65d4-br9qg
updated coredns-5c98db65d4-7wtns
"""

import asyncio
import signal

from kubernetes_asyncio import config

from aiok8s.informers import factory


class Handler:
    async def on_add(self, obj):
        print("added", obj.metadata.name)

    async def on_update(self, old_obj, new_obj):
        print("updated", old_obj.metadata.name)

    async def on_delete(self, obj):
        print("deleted", obj.metadata.name)


async def _run():
    await config.load_kube_config()
    informer_factory = factory.new(namespace="kube-system")
    pod_informer = informer_factory.pods().informer()
    await pod_informer.add_event_handler(Handler())

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_ in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_, stop.set)

    informer_factory.start(stop)
    await informer_factory.wait_for_cache_sync()
    await informer_factory.join()


if __name__ == "__main__":
    asyncio.run(_run())
