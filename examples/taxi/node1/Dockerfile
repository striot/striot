FROM striot/striot-base:latest
WORKDIR /opt/client
COPY . /opt/client
RUN ghc node1.hs
CMD ["/bin/sh", "-c", "/opt/client/node1 < /opt/client/sorteddata.csv"]
