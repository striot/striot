#Run from a pre-built Haskell container
FROM haskell:7.10

WORKDIR /opt/client2

RUN apt-get -yqq update && apt-get -yqq install git

# Install the network stack from Cabal as this isn't part of the core
RUN cabal update
RUN cabal install network split stm

# Add and Install Application Code
COPY . /opt/client2

RUN mkdir ~/.ssh/
RUN mv /opt/client2/id_rsa ~/.ssh/id_rsa
RUN chmod 600 ~/.ssh/id_rsa

# Add the BitBucket ssh keys
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan bitbucket.org >> /root/.ssh/known_hosts

# Checkout the striot code from Git and build
RUN git clone git@bitbucket.org:digitalinstitute/haskell.git
WORKDIR /opt/client2/haskell
RUN cabal build && cabal install
WORKDIR /opt/client2

# Compile
RUN ghc Client2.hs

# Run the client 
CMD ["/opt/client2/Client2"]

