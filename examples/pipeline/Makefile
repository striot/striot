GHC := stack ghc -- -XTemplateHaskell

run: nodes
	docker-compose up --build

nodes: node1/node.hs node2/node.hs node3/node.hs

node1 node2 node3 node1/node.hs node2/node.hs node3/node.hs: generate
	./generate

generate: generate.hs
	$(GHC) -i../../src generate.hs

clean:
	rm -f generate generate.hi generate.o \
	    generate.dyn_hi generate.dyn_o \
	    node1/node.hs node2/node.hs node3/node.hs \
	    node1/node.hi node2/node.hi node3/node.hi \
	    node1/node.o node2/node.o node3/node.o \
	    node1/node node2/node node3/node \
	    node1/Dockerfile node2/Dockerfile node3/Dockerfile \
	    compose.yml
	-rmdir node1 node2 node3

.PHONY: clean nodes
