FROM striot/striot-base:latest
WORKDIR /opt/server
COPY . /opt/server
RUN ghc node3.hs
EXPOSE 9001
CMD /opt/server/node3
