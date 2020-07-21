#!/bin/sh
## Origin: leading-house-mena-innogrant/logdata/c-accel/pulsar
#
# http://pulsar.apache.org/docs/en/standalone-docker/
# + pip3 install pulsar-client
# 

docker run -it \
  --rm \
  -p 6650:6650 \
  -p 8080:8080 \
  --name pulsar-standalone \
  apachepulsar/pulsar-all:2.5.2 \
  bin/pulsar standalone

  #--mount source=pulsardata,target=/pulsar/data \
  #--mount source=pulsarconf,target=/pulsar/conf \
