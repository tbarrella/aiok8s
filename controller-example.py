"""

Example using a controller.

Usage:
kind create cluster
python controller-example.py

Output:
kube-proxy-c7d7p
kindnet-t5bmr
coredns-5c98db65d4-qdv6t
kube-apiserver-kind-control-plane
kube-scheduler-kind-control-plane
etcd-kind-control-plane
coredns-5c98db65d4-br9qg
kube-controller-manager-kind-control-plane
"""

import asyncio
import signal

from kubernetes_asyncio import client, config

from aiok8s.controller import builder, manager, reconcile


class Reconciler:
    async def reconcile(self, request):
        print(request.name)
        return reconcile.Result()


async def _run():
    await config.load_kube_config()
    mgr = manager.new()
    await builder.build_controller(mgr, Reconciler(), api_type=client.V1Pod)
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
