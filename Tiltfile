"""
Tilt config for pg-queue.

Cluster: k3d. Images are pushed to a k3d-managed local registry container
(created via `k3d cluster create ... --registry-create pgregistry:0.0.0.0:5111`).
Tilt pushes to `localhost:5111`; cluster nodes pull from `pgregistry:5000`
over k3d's docker network.

One image (`pg-queue-app`) contains both binaries. Server and worker
deployments run different entrypoints from the same image.
"""

default_registry("localhost:5111", host_from_cluster="pgregistry:5000")

docker_build(
    "pg-queue-app",
    context=".",
    dockerfile="Dockerfile",
)

k8s_yaml([
    "k8s/postgres.yaml",
    "k8s/server.yaml",
    "k8s/worker.yaml",
])

k8s_resource("postgres", port_forwards="5432:5432", labels=["db"])
k8s_resource("server", port_forwards="3000:3000", resource_deps=["postgres"], labels=["app"])
k8s_resource("worker", resource_deps=["postgres"], labels=["app"])
