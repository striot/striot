FROM debian:10-slim
WORKDIR /opt/node
RUN apt-get update \
    && apt-get install --no-install-recommends -y librdkafka1 \
    && apt-get clean
USER nobody
