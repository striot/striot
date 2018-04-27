run: nodes
	docker-compose up --build

nodes: node1/node1.hs node2/node2.hs node3/node3.hs

node1/node1.hs node2/node2.hs node3/node3.hs: generate
	./generate

generate: generate.hs
	ghc generate -i../../src

clean:
	rm -f generate generate.hi generate.o node1/node1.hs node2/node2.hs node3/node3.hs

.PHONY: clean nodes
