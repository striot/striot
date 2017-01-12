# Run from a pre-built Haskell container
FROM sjwoodman/striot-base:0.1

# Add and Install Application Code
COPY . /opt/haskell

WORKDIR /opt/haskell

# Compile
RUN ghc Main.hs

# Run the wrapper
CMD /opt/wrapper/bin/startserver.sh sink
