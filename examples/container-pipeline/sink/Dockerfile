# Run from a pre-built Haskell container
FROM ghcr.io/striot/striot:latest

# Add and Install Application Code
COPY . /opt/haskell

WORKDIR /opt/haskell

# Compile
RUN ghc Main.hs

CMD /opt/haskell/Main
