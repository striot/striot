run: nodes
	docker-compose up --build

gen: generate
	./generate

nodes: node1/node.hs node2/node.hs node3/node.hs node1/Taxi.hs node2/Taxi.hs node3/Taxi.hs

node1/Taxi.hs: Taxi.hs
	cp Taxi.hs node1

node2/Taxi.hs: Taxi.hs
	cp Taxi.hs node2

node3/Taxi.hs: Taxi.hs
	cp Taxi.hs node3

node2 node3 node1/node.hs node2/node.hs node3/node.hs: generate
	./generate

generate: generate.hs
	stack ghc -- -i../../src generate.hs

clean:
	rm -f generate generate.hi generate.o \
	    Taxi.hi Taxi.o \
	    node1/node.hs node2/node.hs node3/node.hs \
	    node1/node.hi node2/node.hi node3/node.hi \
	    node1/node.o node2/node.o node3/node.o \
	    node1/Taxi.hs node2/Taxi.hs node3/Taxi.hs \
	    node1/Dockerfile node2/Dockerfile node3/Dockerfile
	-test ! -d node2 || rmdir node2
	-test ! -d node3 || rmdir node3

.PHONY: clean nodes gen
