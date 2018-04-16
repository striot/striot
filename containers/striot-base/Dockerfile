# Run from a pre-built Haskell container
FROM haskell:8.0.1

RUN apt-get update && apt-get upgrade -y

# Install the network stack from Cabal as this isn't part of the core
RUN cabal update && \
    cabal install \
    network \
    split \
    stm \
    HTF \
    aeson

COPY striot /opt/striot

WORKDIR /opt/striot
RUN cabal build && \
    cabal install
