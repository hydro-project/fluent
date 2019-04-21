#!/bin/bash
kubectl delete pods scheduler-pod-1 &
kubectl delete pods scheduler-pod-2 &
kubectl delete pods function-pod-1 &
kubectl delete pods function-pod-2 &
kubectl delete pods function-pod-3 &