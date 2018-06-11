FROM striot/striot-base:latest
WORKDIR /opt/server
COPY . /opt/server
RUN cabal install random
RUN ghc node2.hs
EXPOSE 9001
CMD /opt/server/node2
