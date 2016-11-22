#!/bin/bash

echo "Copying wrapper"
mkdir -p striot-base/bin
cp -R ../../haskell-wrapper/bin/* striot-base/bin;
mkdir -p striot-base/lib
cp -R ../../haskell-wrapper/lib/* striot-base/lib;

echo "Copying Striot"
mkdir -p striot-base/striot/src
cp -R ../license.txt striot-base/striot/
cp -R ../striot.cabal striot-base/striot/
cp -R ../src/* striot-base/striot/src

echo "Building"
docker build -t sjwoodman/striot-base striot-base