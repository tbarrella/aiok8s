import asyncio
import signal

from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.client.models import V1Pod

from aiok8s.cache import shared_informer


"""
kind create cluster, then run

Output...
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


def run():
    asyncio.run(_run())


class Handler:
    async def on_add(self, obj):
        print("added", obj.metadata.name)

    async def on_update(self, old_obj, new_obj):
        print("updated", old_obj.metadata.name)

    async def on_delete(self, obj):
        print("deleted", obj.metadata.name)


async def _run():
    informer = new_pod_informer("kube-system", 60)
    await informer.add_event_handler_with_resync_period(Handler(), 60)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_ in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_, stop.set)

    print("running")
    await informer.run(stop)


class ListWatch:
    def __init__(self, namespace):
        self._namespace = namespace

    async def list(self, options):
        print("listing")
        await config.load_kube_config()
        v1 = client.CoreV1Api()
        return await v1.list_namespaced_pod(self._namespace, **options)

    async def watch(self, options):
        print("creating watcher")
        await config.load_kube_config()
        v1 = client.CoreV1Api()
        return watch.Watch().stream(v1.list_namespaced_pod, self._namespace, **options)


def new_pod_informer(namespace, resync_period):
    return shared_informer.new_shared_informer(
        ListWatch(namespace), V1Pod(), resync_period
    )


if __name__ == "__main__":
    run()
