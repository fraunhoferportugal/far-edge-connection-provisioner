#!/bin/bash
microk8s kubectl apply -f temperature1-pod.yaml
microk8s kubectl apply -f temperature2-pod.yaml
microk8s kubectl apply -f avg-temperature-pod.yaml

sleep 5

microk8s kubectl apply -f temperature1-single-route.yaml
microk8s kubectl apply -f temperature2-single-route.yaml
