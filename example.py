import asyncio
from concurrent.futures import ThreadPoolExecutor

from kubernetes import client, config, watch
from kubernetes.client.models import V1Pod

from aiok8s.cache import shared_informer
from aiok8s.watch.watch import Event


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
    print("running")
    try:
        await informer.run(stop)
    finally:
        stop.set()


class ListWatch:
    def __init__(self, namespace):
        self._namespace = namespace

    async def list(self, **options):
        def list_pods():
            config.load_kube_config()
            v1 = client.CoreV1Api()
            print("listing")
            return v1.list_namespaced_pod(self._namespace, **options)

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            pod_list = await loop.run_in_executor(pool, list_pods)
            print("got pods", *(pod.metadata.name for pod in pod_list.items))
            return pod_list

    async def watch(self, **options):
        print("creating watcher")
        return Watch(self._namespace, **options)


def new_pod_informer(namespace, resync_period):
    return shared_informer.new_shared_informer(
        ListWatch(namespace), V1Pod(), resync_period
    )


class Watch:
    def __init__(self, namespace, **options):
        config.load_kube_config()
        v1 = client.CoreV1Api()
        self._stopped = asyncio.Event()
        self._task = asyncio.ensure_future(self._stopped.wait())
        self._w = watch.Watch()
        print("watching with options", options)
        self._stream = self._w.stream(v1.list_namespaced_pod, namespace, **options)

    def __aiter__(self):
        return self

    async def __anext__(self):
        event = asyncio.ensure_future(self._get_next())
        while True:
            done, _ = await asyncio.wait(
                [event, self._task], return_when=asyncio.FIRST_COMPLETED
            )
            if event in done:
                return await event
            if self._stopped.is_set():
                raise StopAsyncIteration

    async def stop(self):
        self._w.stop()
        self._stopped.set()

    async def _get_next(self):
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            return await loop.run_in_executor(pool, self._get_event)

    def _get_event(self):
        try:
            event = next(self._stream)
        except StopIteration:
            raise StopAsyncIteration
        return Event(event["type"], event["object"])


if __name__ == "__main__":
    run()
