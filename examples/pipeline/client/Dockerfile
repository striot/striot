#Run from a pre-built Haskell container
FROM jmtd/striot-base:latest

WORKDIR /opt/client

# Add and Install Application Code
COPY . /opt/client

# Compile
RUN ghc Client.hs

# Run the client 
CMD ["/opt/client/Client"]

