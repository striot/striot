# Run from a pre-built Haskell container
FROM haskell:8.6

RUN apt-get update && apt-get upgrade -y

ENV LD_LIBRARY_PATH="/usr/local/lib"

# Install the librdkafka library for Kafka
RUN git clone https://github.com/edenhill/librdkafka && \
    cd librdkafka && \
    git checkout $(git describe --tags `git rev-list --tags --max-count=1`) && \
    ./configure --install-deps && \
    make && make install

# Copy cabal file and license
COPY striot/striot.cabal striot/LICENSE /opt/striot/

WORKDIR /opt/striot

# Install all dependencies defined in cabal file - we must install happy
# separately for HTF, as the image has a bootstrapping problem
RUN cabal v1-update && \
    cabal v1-install happy-1.19.12 alex && \
    cabal v1-install c2hs && \
    cabal v1-install --only-dependencies --extra-include-dirs=/usr/local/include --extra-lib-dirs=/usr/local/lib

# Copy over source code in a separate layer, ensuring above is cached if
# cabal file does not change
COPY striot/src /opt/striot/src

RUN cabal v1-build && \
    cabal v1-install
