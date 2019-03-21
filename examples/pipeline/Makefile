run: nodes
	docker-compose up --build

nodes: node1/node.hs node2/node.hs node3/node.hs

node1 node2 node3 node1/node.hs node2/node.hs node3/node.hs: generate
	./generate

generate: generate.hs
	ghc generate -i../../src

clean:
	rm -f generate generate.hi generate.o \
	    node1/node.hs node2/node.hs node3/node.hs \
	    node1/node node2/node node3/node \
	    node1/Dockerfile node2/Dockerfile node3/Dockerfile
	-rmdir node1 node2 node3

.PHONY: clean nodes
