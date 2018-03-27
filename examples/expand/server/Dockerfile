FROM striot/striot-base:latest
WORKDIR /opt/server
COPY . /opt/server
RUN ghc Server.hs
EXPOSE 9001
CMD /opt/server/Server
