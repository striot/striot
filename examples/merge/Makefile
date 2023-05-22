GHC := stack ghc -- -XTemplateHaskell

run: nodes
	docker-compose up --build

nodes: node1/node.hs node2/node.hs node3/node.hs node4/node.hs

node1 node2 node3 node4 node1/node.hs node2/node.hs node3/node.hs node4/node4.hs: generate
	./generate

generate: generate.hs
	$(GHC) -i../../src generate.hs

clean:
	rm -f generate generate.hi generate.o \
	    generate.dyn_hi generate.dyn_o \
	    node1/Dockerfile node1/node.hs node1/node \
	    node2/Dockerfile node2/node.hs node2/node \
	    node3/Dockerfile node3/node.hs node3/node \
	    node4/Dockerfile node4/node.hs node4/node \
	    compose.yml
	-rmdir node1 node2 node3 node4

.PHONY: clean nodes
