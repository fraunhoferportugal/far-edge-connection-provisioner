#!/bin/bash
microk8s kubectl delete pod temperature-1 
microk8s kubectl delete pod temperature-2
microk8s kubectl delete pod average-temperature