#!/bin/sh
# A kind cluster for the e2e tests' Kubernetes targets: the Flink operator,
# without its webhook, watching one namespace, and Spark's service account
# there. Its kubeconfig goes to KIND_KUBECONFIG, never to the default one.
#
# Spark's pods read and write the runner's store where the node mounts
# OPENETL_SPARK_SHARE, at the same path, and spark-submit runs in the
# openetl-kind-spark-submit container on the kind network (spark-submit
# beside this file).
set -eu

name=${KIND_NAME:-openetl}
kubeconfig=${KIND_KUBECONFIG:-.openetl/kind/kubeconfig}
namespace=${KIND_NAMESPACE:-openetl}
operator=${FLINK_OPERATOR_VERSION:-1.16.1}
flink=${FLINK_IMAGE:-flink:1.20.5}
spark=${SPARK_IMAGE:-apache/spark:3.5.8}
share=${OPENETL_SPARK_SHARE:-/tmp/openetl-spark}

mkdir -p "$(dirname "$kubeconfig")" "$share"
if kind get clusters 2>/dev/null | grep -qx "$name"; then
	kind export kubeconfig --name "$name" --kubeconfig "$kubeconfig"
else
	printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n- role: control-plane\n  extraMounts:\n  - hostPath: %s\n    containerPath: %s\n' "$share" "$share" |
		kind create cluster --name "$name" --kubeconfig "$kubeconfig" --wait 2m --config -
fi
k() { kubectl --kubeconfig "$kubeconfig" "$@"; }

# kind load fails on Docker's containerd image store, so the node pulls them.
docker exec "$name-control-plane" crictl pull "$flink" >/dev/null
docker exec "$name-control-plane" crictl pull "$spark-python3" >/dev/null

k create namespace "$namespace" --dry-run=client -o yaml | k apply -f -
k -n "$namespace" create serviceaccount spark --dry-run=client -o yaml | k apply -f -
k -n "$namespace" create rolebinding spark --clusterrole=edit --serviceaccount="$namespace:spark" --dry-run=client -o yaml | k apply -f -
helm upgrade --install flink-kubernetes-operator \
	"https://downloads.apache.org/flink/flink-kubernetes-operator-$operator/flink-kubernetes-operator-$operator-helm.tgz" \
	--kubeconfig "$kubeconfig" --namespace flink-operator --create-namespace \
	--set webhook.create=false --set "watchNamespaces={$namespace}" --wait --timeout 5m

kind get kubeconfig --internal --name "$name" > "$kubeconfig-internal"
docker rm -f "$name-kind-spark-submit" >/dev/null 2>&1 || true
docker run -d --name "$name-kind-spark-submit" --network kind --user root \
	-v "$share:$share" -v "$(cd "$(dirname "$kubeconfig")" && pwd)/$(basename "$kubeconfig")-internal:/kube/config:ro" \
	-e KUBECONFIG=/kube/config "$spark" sleep infinity >/dev/null
echo "kind cluster $name is up; KUBECONFIG=$kubeconfig, namespace $namespace"
