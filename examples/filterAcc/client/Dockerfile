FROM striot/striot-base:latest
WORKDIR /opt/client
COPY . /opt/client
RUN cabal install random
RUN ghc Client.hs
CMD ["/opt/client/Client"]
