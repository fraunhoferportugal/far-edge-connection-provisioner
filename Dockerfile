FROM python:3.12-slim

RUN pip install kopf
RUN pip install kubernetes
RUN pip install paho.mqtt==1.6.1

ADD operator /far-edge-connection-provisioner

CMD kopf run /far-edge-connection-provisioner/route_operator.py --verbose