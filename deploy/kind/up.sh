#!/bin/sh
# A kind cluster for the e2e tests' Kubernetes targets: the Flink operator,
# without its webhook, watching one namespace. Its kubeconfig goes to
# KIND_KUBECONFIG, never to the default one.
set -eu

name=${KIND_NAME:-openetl}
kubeconfig=${KIND_KUBECONFIG:-.openetl/kind/kubeconfig}
namespace=${KIND_NAMESPACE:-openetl}
operator=${FLINK_OPERATOR_VERSION:-1.16.1}
flink=${FLINK_IMAGE:-flink:1.20.5}

mkdir -p "$(dirname "$kubeconfig")"
if kind get clusters 2>/dev/null | grep -qx "$name"; then
	kind export kubeconfig --name "$name" --kubeconfig "$kubeconfig"
else
	kind create cluster --name "$name" --kubeconfig "$kubeconfig" --wait 2m
fi
k() { kubectl --kubeconfig "$kubeconfig" "$@"; }

# kind load fails on Docker's containerd image store, so the node pulls it.
docker exec "$name-control-plane" crictl pull "$flink" >/dev/null

k create namespace "$namespace" --dry-run=client -o yaml | k apply -f -
helm upgrade --install flink-kubernetes-operator \
	"https://downloads.apache.org/flink/flink-kubernetes-operator-$operator/flink-kubernetes-operator-$operator-helm.tgz" \
	--kubeconfig "$kubeconfig" --namespace flink-operator --create-namespace \
	--set webhook.create=false --set "watchNamespaces={$namespace}" --wait --timeout 5m
echo "kind cluster $name is up; KUBECONFIG=$kubeconfig, namespace $namespace"
