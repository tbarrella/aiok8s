"""

Example using a controller.

Usage:
kind create cluster
python controller-example.py

Output:
kube-system/kube-controller-manager-kind-control-plane: ip 172.17.0.2
kube-system/kube-proxy-zvl88: ip 172.17.0.2
kube-system/kube-apiserver-kind-control-plane: ip 172.17.0.2
kube-system/kube-scheduler-kind-control-plane: ip 172.17.0.2
kube-system/etcd-kind-control-plane: ip 172.17.0.2
kube-system/coredns-5644d7b6d9-l9vhx: ip 10.244.0.35
kube-system/coredns-5644d7b6d9-zj2rw: ip 10.244.0.36
kube-system/kindnet-85dqj: ip 172.17.0.2
kube-system/coredns-5644d7b6d9-l9vhx: ip 10.244.0.35
kube-system/coredns-5644d7b6d9-fh7l8: ip None
kube-system/coredns-5644d7b6d9-zj2rw: ip 10.244.0.36
kube-system/coredns-5644d7b6d9-fh7l8: ip None
kube-system/coredns-5644d7b6d9-dvf8h: ip None
kube-system/coredns-5644d7b6d9-dvf8h: ip None
kube-system/coredns-5644d7b6d9-fh7l8: ip None
kube-system/coredns-5644d7b6d9-dvf8h: ip None
kube-system/coredns-5644d7b6d9-l9vhx: ip 10.244.0.35
kube-system/coredns-5644d7b6d9-zj2rw: ip 10.244.0.36
kube-system/coredns-5644d7b6d9-fh7l8: ip 10.244.0.37
kube-system/coredns-5644d7b6d9-dvf8h: ip 10.244.0.38
kube-system/coredns-5644d7b6d9-dvf8h: ip 10.244.0.38
kube-system/coredns-5644d7b6d9-zj2rw: ip 10.244.0.36
coredns-5644d7b6d9-zj2rw/coredns-5644d7b6d9-zj2rw deleted
kube-system/coredns-5644d7b6d9-l9vhx: ip 10.244.0.35
coredns-5644d7b6d9-l9vhx/coredns-5644d7b6d9-l9vhx deleted
kube-system/coredns-5644d7b6d9-fh7l8: ip 10.244.0.37
"""

import asyncio
import signal

from kubernetes_asyncio import client, config

from aiok8s.controller import builder, manager, reconcile


class Reconciler:
    def __init__(self, cache):
        self._cache = cache

    async def reconcile(self, request):
        try:
            pod = await self._cache.get(request.namespaced_name, client.V1Pod)
        except KeyError:
            print(
                f"{request.namespaced_name.name}/{request.namespaced_name.name} deleted"
            )
        else:
            print(
                f"{pod.metadata.namespace}/{pod.metadata.name}: ip", pod.status.pod_ip
            )
        return reconcile.Result()


async def _run():
    await config.load_kube_config()
    mgr = manager.new()
    await builder.build_controller(
        mgr, Reconciler(mgr.get_cache()), api_type=client.V1Pod
    )
    task = asyncio.ensure_future(mgr.start())

    loop = asyncio.get_running_loop()
    for signal_ in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_, task.cancel)

    try:
        await task
    except asyncio.CancelledError:
        print("\nInterrupted...")


if __name__ == "__main__":
    asyncio.run(_run())
