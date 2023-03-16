# Run from a pre-built Haskell container
FROM haskell:9.0.2-slim

RUN apt-get update && apt-get upgrade -y

ENV LD_LIBRARY_PATH="/usr/local/lib"
ENV LANG="C.UTF-8"

# Install the librdkafka library for Kafka
RUN apt-get install -y librdkafka-dev

# Copy cabal file and license
COPY striot.cabal LICENSE README.md /opt/striot/

WORKDIR /opt/striot

# Install all dependencies defined in cabal file
RUN cabal update

# happy to resolve HTF bootstrapping
# c2hs for hw-kafka-client
# alex c2hs
RUN cabal v1-install happy alex && \
    cabal v1-install c2hs
RUN cabal v1-install --only-dependencies --extra-include-dirs=/usr/local/include --extra-lib-dirs=/usr/local/lib --minimize-conflict-set


# Copy over source code in a separate layer, ensuring above is cached if
# cabal file does not change
COPY src /opt/striot/src

RUN cabal v1-build && \
    cabal v1-install
