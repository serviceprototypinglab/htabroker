#!/bin/sh
# For use with docker-compose - therefore sleeping 25s to give pulsar the chance to become ready

echo "HTA Broker (container run via compose) warming up..."
sleep 25

cd /opt
python3 htabroker.py
