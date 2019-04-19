#!/bin/bash
kubectl delete pods memory-pod-1 &
kubectl delete pods memory-pod-2 &
kubectl delete pods memory-pod-3 &
kubectl delete pods memory-pod-4 &
kubectl delete pods memory-pod-5 &
kubectl delete pods routing-pod-1 &
kubectl delete pods routing-pod-2 &
kubectl delete pods kops-pod &
kubectl delete pods monitoring-pod &
kubectl delete pods scheduler-pod-1 &
kubectl delete pods scheduler-pod-2 &
kubectl delete pods function-pod-1 &
kubectl delete pods function-pod-2 &
kubectl delete pods function-pod-3 &
kubectl delete pods benchmark-pod-1 &
kubectl delete pods benchmark-pod-2 &