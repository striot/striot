GHC := stack ghc -- -XTemplateHaskell

run: nodes
	docker-compose up --build

gen: generate
	./generate

nodes: node1/node.hs node2/node.hs node3/node.hs node1/Taxi.hs node2/Taxi.hs node3/Taxi.hs node1/sorteddata.csv

node1/Taxi.hs: Taxi.hs
	cp Taxi.hs node1

node2/Taxi.hs: Taxi.hs
	cp Taxi.hs node2

node3/Taxi.hs: Taxi.hs
	cp Taxi.hs node3

node2 node3 node1/node.hs node2/node.hs node3/node.hs: generate
	./generate

generate: generate.hs
	$(GHC) -i../../src generate.hs

# This file is deliberately left out of the "clean" target.
sorteddata.csv.xz:
	curl https://www.staff.ncl.ac.uk/jon.dowland/phd/debs2015/sorteddata.csv.xz >$@

node1/sorteddata.csv: sorteddata.csv.xz
	xzcat $< >$@

clean:
	rm -f generate generate.hi generate.o \
	    Taxi.hi Taxi.o \
	    node1/sorteddata.csv \
	    node1/node.hs node2/node.hs node3/node.hs \
	    node1/node.hi node2/node.hi node3/node.hi \
	    node1/node node2/node node3/node \
	    node1/node.o node2/node.o node3/node.o \
	    node1/Taxi.hs node2/Taxi.hs node3/Taxi.hs \
	    node1/Dockerfile node2/Dockerfile node3/Dockerfile
	-rmdir node1 node2 node3

.PHONY: clean nodes gen
