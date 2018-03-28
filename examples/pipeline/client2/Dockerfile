#Run from a pre-built Haskell container
FROM striot/striot-base:latest

WORKDIR /opt/client2


# Add and Install Application Code
COPY . /opt/client2

# Compile
RUN ghc Client2.hs

# Run the client 
CMD ["/opt/client2/Client2"]

