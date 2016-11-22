#Run from a pre-built Haskell container
FROM haskell:7.10

WORKDIR /opt/link

RUN apt-get -yqq update && apt-get -yqq install git

# Install the network stack from Cabal as this isn't part of the core
RUN cabal update
RUN cabal install network split stm

# Add and Install Application Code
COPY . /opt/link

RUN mkdir ~/.ssh/
RUN mv /opt/link/id_rsa ~/.ssh/id_rsa
RUN chmod 600 ~/.ssh/id_rsa

# Add the BitBucket ssh keys
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan bitbucket.org >> /root/.ssh/known_hosts

# Checkout the striot code from Git and build
RUN git clone git@bitbucket.org:digitalinstitute/haskell.git
WORKDIR /opt/link/haskell
RUN cabal build && cabal install
WORKDIR /opt/link

# Compile
RUN ghc Link.hs

# Run the link
CMD ["/opt/link/Link"]

