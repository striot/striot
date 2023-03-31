FROM ghcr.io/striot/striot:main
WORKDIR /opt/node
COPY . /opt/node

RUN ghc node.hs
CMD /opt/node/node
