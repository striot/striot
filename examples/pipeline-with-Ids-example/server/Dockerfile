# Run from a pre-built Haskell container
FROM haskell:7.10

WORKDIR /opt/server

# Install the network stack from Cabal as this isn't part of the core
RUN cabal update
RUN cabal install network

# Add and Install Application Code
COPY . /opt/server

# Compile
RUN ghc Server.hs

# Expose the port so that we can talk to the server
EXPOSE 9001

# Run the server
CMD /opt/server/Server
