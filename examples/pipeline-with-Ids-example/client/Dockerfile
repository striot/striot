#Run from a pre-built Haskell container
FROM haskell:7.10

WORKDIR /opt/client

# Install the network stack from Cabal as this isn't part of the core
RUN cabal update
RUN cabal install network

# Add and Install Application Code
COPY . /opt/client

# Compile
RUN ghc Client.hs

# Run the client 
CMD ["/opt/client/Client"]

