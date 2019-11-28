# Messy example, for development
import asyncio
from concurrent.futures import ThreadPoolExecutor

from kubernetes import client, config, watch
from kubernetes.client.models import V1Pod

from aiok8s.cache import controller, index, shared_informer


"""
kind create cluster, then run

Output...
adding kube-proxy-c7d7p
adding kindnet-t5bmr
adding coredns-5c98db65d4-qdv6t
adding kube-apiserver-kind-control-plane
adding kube-scheduler-kind-control-plane
adding etcd-kind-control-plane
adding coredns-5c98db65d4-br9qg
adding kube-controller-manager-kind-control-plane
updating coredns-5c98db65d4-br9qg
updating coredns-5c98db65d4-br9qg
adding coredns-5c98db65d4-7wtns
updating coredns-5c98db65d4-7wtns
updating coredns-5c98db65d4-7wtns
updating coredns-5c98db65d4-7wtns
updating coredns-5c98db65d4-br9qg
updating coredns-5c98db65d4-br9qg
updating coredns-5c98db65d4-br9qg
deleting coredns-5c98db65d4-br9qg
updating coredns-5c98db65d4-7wtns
"""


def run():
    asyncio.run(_run())


async def _run():
    async def add_func(obj):
        print("adding", obj.metadata.name)

    async def update_func(old_obj, new_obj):
        print("updating", old_obj.metadata.name)

    async def delete_func(obj):
        print("deleting", obj.metadata.name)

    inf = new_pod_informer("kube-system", 60)
    handler = controller.ResourceEventHandlerFuncs(add_func, update_func, delete_func)
    await inf.add_event_handler_with_resync_period(handler, 60)
    stop = asyncio.Event()
    print("running")
    try:
        await inf.run(stop)
    finally:
        stop.set()


def new_pod_informer(namespace, resync_period):
    class ListWatch:
        async def list(self, **options):
            def list_():
                config.load_kube_config()
                v1 = client.CoreV1Api()
                print("listing")
                return v1.list_namespaced_pod(namespace, **options)

            print("listing in another thread")
            try:
                loop = asyncio.get_running_loop()
                with ThreadPoolExecutor() as pool:
                    ret = await loop.run_in_executor(pool, list_)
                    print("got list", *(i.metadata.name for i in ret.items))
                    return ret
            except Exception as e:
                print(repr(e))

        async def watch(self, **options):
            print("returning watcher")
            return Watch(namespace, **options)

    return shared_informer.new_shared_index_informer(
        ListWatch(), V1Pod(), resync_period, index.Indexers()
    )


class Watch:
    def __init__(self, namespace, **options):
        config.load_kube_config()
        v1 = client.CoreV1Api()
        self._stopped = asyncio.Event()
        self._task = asyncio.ensure_future(self._stopped.wait())
        self._w = watch.Watch()
        print("OPTIONS", options)
        self._stream = self._w.stream(v1.list_namespaced_pod, namespace, **options)
        self._mutex = asyncio.Lock()

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

    async def _get_next(self):
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            # TODO: Handle StopIteration
            return await loop.run_in_executor(pool, lambda: next(self._stream))


if __name__ == "__main__":
    run()
