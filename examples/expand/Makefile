run: node1/node.hs node2/node.hs
	docker-compose up --build

node1 node2 node1/node.hs node2/node.hs: generate
	./generate

generate: generate.hs
	ghc generate -i../../src

clean:
	rm -f generate generate.hi generate.o node1/node.hs node2/node.hs\
	    node1/node node2/node \
	    node1/Dockerfile node2/Dockerfile
	-rmdir node1 node2

.PHONY: clean
