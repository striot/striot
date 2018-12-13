# Run from a pre-built Haskell container
FROM haskell:8.4

RUN apt-get update && apt-get upgrade -y

# Copy cabal file and license
COPY striot/striot.cabal striot/LICENSE /opt/striot/

WORKDIR /opt/striot

# Install all dependencies defined in cabal file - we must install happy
# separately for HTF, as the image has a bootstrapping problem
RUN cabal update && \
    cabal install happy && \
    cabal install --only-dependencies

# Copy over source code in a separate layer, ensuring above is cached if
# cabal file does not change
COPY striot/src /opt/striot/src

RUN cabal build && \
    cabal install
