run: node1/node1.hs node2/node2.hs
	docker-compose up --build

node1/node1.hs node2/node2.hs: generate
	./generate

generate: generate.hs
	ghc generate -i../../src

clean:
	rm -f generate generate.hi generate.o node1/node1.hs node2/node2.hs

.PHONY: clean
