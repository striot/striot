FROM striot/striot-base:latest
WORKDIR /opt/server
COPY . /opt/server
RUN ghc node2.hs
CMD ["/opt/server/node2"]
