#Run from a pre-built Haskell container
FROM sjwoodman/striot-base:0.1

WORKDIR /opt/client2


# Add and Install Application Code
COPY . /opt/client2

# Compile
RUN ghc Client2.hs

# Run the client 
CMD ["/opt/client2/Client2"]

