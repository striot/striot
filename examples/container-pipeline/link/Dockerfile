# Run from a pre-built Haskell container
FROM striot/striot-base:latest

# Add and Install Application Code
COPY . /opt/haskell

WORKDIR /opt/haskell

# Compile
RUN ghc Main.hs

CMD /opt/haskell/Main
