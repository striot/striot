# StrIoT — functional stream processing for IoT

[![Travis CI Build Status](https://api.travis-ci.org/striot/striot.svg?branch=master)](https://travis-ci.org/striot/striot)

**StrIoT** is a stream-processing engine for IoT workloads, implemented as a
[Haskell](https://www.haskell.org) library. A user defines a stream-processing
program using a set of operators provided by *StrIoT* and the result is
rewritten/optimised and partitioned into distinct sub-programs that can be
deployed to separate processing nodes, connected together via TCP/IP.

*StrIoT* is experimental software being developed within a research project in
the [Scalable Systems Group](https://www.ncl.ac.uk/computing/research/groups/scalable/),
[School of Computing](https://www.ncl.ac.uk/computing/),
[Newcastle University](https://www.ncl.ac.uk/) (UK).

## Building StrIoT

We recommend using [Stack](https://haskellstack.org): `stack build && stack
install`, but it is also possible to use [Cabal](http://www.haskell.org/cabal/)
directly: `cabal build && cabal install`.

## Using StrIoT

*StrIoT* can be used in several ways:

### Using *StrIoT* Operators in Haskell

You can define a stream-processing program directly in terms of the provided
operators. See [docs/Operators.md](docs/Operators.md) for a detailed
description of the operators and several example programs. 

[Haddock](https://www.haskell.org/haddock/)-generated API documentation is
[published here](https://redmars.org/striot/).  This is very rudimentary,
patches to improve it are welcome!

### For use with the Optimiser

*StrIoT*'s optimiser system takes as input a graph data-structure describing
the stream-processing program, which is then processed and converted into
distinct Haskell programs that can be individually compiled and executed.
The API is described in the Haddock [Haddock
documentation](https://redmars.org/striot/). For more detailed examples, see
the [examples/](examples/) sub-directory.

### Using Docker

Our [examples/](examples/) use [Docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) for executing the programs
output by the Optimiser.

We publish a base Docker image containing the *StrIoT* library to the Docker Hub:
<https://hub.docker.com/u/striot>

## Contributing

Contributions are welcome! We use our [GitHub project](https://github.com/striot/striot)
to track issues and manage Pull Requests. We use a low-traffic mailing list for
discussions about the project: <https://lists.ncl.ac.uk/wws/info/striot>

## Copyright & License

StrIoT is Copyright © 2020 Adam Cattermole, Jonathan Dowland, Paul Watson and
others. StrIoT is made available under the Apache License, Version 2. See
[LICENSE](LICENSE) for the full text.
