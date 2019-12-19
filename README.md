# aiok8s

This a client library meant for building asynchronous Kubernetes controllers
using Python. It currently integrates with
[`kubernetes_asyncio`](https://github.com/tomplus/kubernetes_asyncio).

Most of the code is directly translated from and attributed to
[`client-go`](https://github.com/kubernetes/client-go) and
[`controller-runtime`](https://github.com/kubernetes-sigs/controller-runtime).
This approach is taken to help ensure that the logic is battle-tested and
reduce overhead in maintaining the library. It's meant to replicate only some
of the most desirable APIs of the Go libraries, not all of them.

This is in early development. APIs may be adjusted or even removed.
