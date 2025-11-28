# Usage:
#   bash scripts/debug_pod.sh <namespace> <pod-name>
# If you don't know the pod name, use 'kubectl get pods -n <namespace>'
#
# Alternative: For deployments, get the pod name with:
#   POD=$(kubectl get pods -n <namespace> -l app=<deployment-name> -o jsonpath='{.items[0].metadata.name}')
#   bash scripts/debug_pod.sh <namespace> $POD

NAMESPACE="dev"
POD_NAME="datagen-api-77f7b789-tzjxp"

if [ -z "$POD_NAME" ]; then
  echo "Usage: debug_pod.sh <namespace> <pod-name>"
  exit 1
fi

echo "Describing pod..."
# kubectl describe pod $POD_NAME -n $NAMESPACE

echo "Getting pod logs (all containers)..."
kubectl logs $POD_NAME -n $NAMESPACE --all-containers=true

echo "Attaching shell (if possible)..."
# kubectl exec -it $POD_NAME -n $NAMESPACE -- sh || kubectl exec -it $POD_NAME -n $NAMESPACE -- bash

