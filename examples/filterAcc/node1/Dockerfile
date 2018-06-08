FROM striot/striot-base:latest
WORKDIR /opt/client
COPY . /opt/client
RUN cabal install random
RUN ghc node1.hs
CMD ["/opt/client/node1"]
