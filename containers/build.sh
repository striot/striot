#!/bin/bash

prefix=${prefix-sjwoodman}

echo "Copying Striot"
mkdir -p striot-base/striot/src
cp -R ../license.txt striot-base/striot/
cp -R ../striot.cabal striot-base/striot/
cp -R ../src/* striot-base/striot/src

echo "Building"
docker build -t $prefix/striot-base striot-base
