#!/bin/bash

kubectl cp ~/.aws benchmark-pod-1:/root/.aws -c benchmark-1
kubectl cp ~/.aws benchmark-pod-1:/root/.aws -c benchmark-2
kubectl cp ~/.aws benchmark-pod-1:/root/.aws -c benchmark-3
kubectl cp ~/.aws benchmark-pod-1:/root/.aws -c benchmark-4
kubectl cp ~/.aws benchmark-pod-2:/root/.aws -c benchmark-4
kubectl cp ~/.aws benchmark-pod-2:/root/.aws -c benchmark-3
kubectl cp ~/.aws benchmark-pod-2:/root/.aws -c benchmark-2
kubectl cp ~/.aws benchmark-pod-2:/root/.aws -c benchmark-1
kubectl cp ~/.aws benchmark-pod-3:/root/.aws -c benchmark-1
kubectl cp ~/.aws benchmark-pod-3:/root/.aws -c benchmark-2
kubectl cp ~/.aws benchmark-pod-3:/root/.aws -c benchmark-3
kubectl cp ~/.aws benchmark-pod-3:/root/.aws -c benchmark-4
