FROM striot/striot-base:latest
WORKDIR /opt/node
COPY . /opt/node

RUN ghc node.hs
EXPOSE 9001
CMD /opt/node/node
