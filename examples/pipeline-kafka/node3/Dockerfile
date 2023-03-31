FROM ghcr.io/striot/striot:main
WORKDIR /opt/node
COPY . /opt/node

RUN ghc node.hs
EXPOSE 9001
CMD /opt/node/node
