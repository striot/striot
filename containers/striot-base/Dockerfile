# Run from a pre-built Haskell container
FROM haskell:7.10

RUN apt-get update && apt-get install -y openjdk-7-jdk

COPY lib /opt/wrapper/lib
COPY bin /opt/wrapper/bin
COPY striot /opt/striot

# Install the network stack from Cabal as this isn't part of the core
RUN cabal update
RUN cabal install network split stm

WORKDIR /opt/striot
RUN cabal build && cabal install

