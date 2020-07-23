#!/bin/sh

origdir=$PWD

cd ~/Dokumente/Pulsar/apache-pulsar-2.5.2

bin/pulsar-admin functions create \
  --py $origdir/test/htaproc.py \
  --classname htaproc \
  --tenant public \
  --namespace default \
  --name htaproc \
  --inputs non-persistent://public/default/b6589fc6ab0dc82cf12099d1c2d40ab994e8410c \
  --output non-persistent://public/default/fb96549631c835eb239cd614cc6b5cb7d295121a \
  --parallelism 1

# 0 (in, pub): b6589fc6ab0dc82cf12099d1c2d40ab994e8410c
# 00 (out, sub): fb96549631c835eb239cd614cc6b5cb7d295121a
