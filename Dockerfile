# Build instructions: docker build -t htabroker .

FROM python:3-slim

LABEL maintainer="spio@zhaw.ch"

RUN pip3 install pulsar-client

COPY htabroker.py /opt/
COPY lib /opt/lib
COPY data /opt/data

CMD ["/opt/lib/htabroker-init.sh"]
