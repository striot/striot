# this file was generated by ./gen_test_makefile.sh

BUILD_TOOL ?= docker
REGISTRY ?= ghcr.io
ORG ?= striot
IMAGE ?= striot
VERSION ?= latest

GHC := stack ghc -- -XTemplateHaskell
default: default2

examples/expand:
	+make -C examples/expand clean GHC="$(GHC)"
	+make -C examples/expand generate GHC="$(GHC)"
	cd "examples/expand" && ./generate

examples/filterAcc:
	+make -C examples/filterAcc clean GHC="$(GHC)"
	+make -C examples/filterAcc generate GHC="$(GHC)"
	cd "examples/filterAcc" && ./generate

examples/filter:
	+make -C examples/filter clean GHC="$(GHC)"
	+make -C examples/filter generate GHC="$(GHC)"
	cd "examples/filter" && ./generate

examples/join:
	+make -C examples/join clean GHC="$(GHC)"
	+make -C examples/join generate GHC="$(GHC)"
	cd "examples/join" && ./generate

examples/merge:
	+make -C examples/merge clean GHC="$(GHC)"
	+make -C examples/merge generate GHC="$(GHC)"
	cd "examples/merge" && ./generate

examples/pipeline:
	+make -C examples/pipeline clean GHC="$(GHC)"
	+make -C examples/pipeline generate GHC="$(GHC)"
	cd "examples/pipeline" && ./generate

examples/scan:
	+make -C examples/scan clean GHC="$(GHC)"
	+make -C examples/scan generate GHC="$(GHC)"
	cd "examples/scan" && ./generate

examples/taxi:
	+make -C examples/taxi clean GHC="$(GHC)"
	+make -C examples/taxi generate GHC="$(GHC)"
	cd "examples/taxi" && ./generate

image/build:
	$(BUILD_TOOL) build -t $(REGISTRY)/$(ORG)/$(IMAGE):$(VERSION) .

clean:
	+make -C examples/expand clean GHC="$(GHC)"
	+make -C examples/filterAcc clean GHC="$(GHC)"
	+make -C examples/filter clean GHC="$(GHC)"
	+make -C examples/join clean GHC="$(GHC)"
	+make -C examples/merge clean GHC="$(GHC)"
	+make -C examples/pipeline clean GHC="$(GHC)"
	+make -C examples/scan clean GHC="$(GHC)"
	+make -C examples/taxi clean GHC="$(GHC)"

docs:
	stack haddock --no-haddock-deps

docs-upload: docs
	rsync -va --delete .stack-work/dist/*/*/doc/html/striot/ redmars.org:www/redmars/striot/

default2: examples/expand examples/filterAcc examples/filter examples/join examples/merge examples/pipeline examples/scan examples/taxi
.PHONY: default default2 clean docs docs-upload image/build examples/expand examples/filterAcc examples/filter examples/join examples/merge examples/pipeline examples/scan examples/taxi
