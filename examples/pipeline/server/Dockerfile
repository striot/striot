# Run from a pre-built Haskell container
FROM striot/striot-base:latest

WORKDIR /opt/server


# Add and Install Application Code
COPY . /opt/server

# Compile
RUN ghc Server.hs

# Expose the port so that we can talk to the server
EXPOSE 9001

# Run the server
CMD /opt/server/Server
