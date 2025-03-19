GHC_PREFIX = stack
GHC := $(GHC_PREFIX) ghc -- -XTemplateHaskell

run: nodes
	docker-compose up --build

gen: Main
	./Main

nodes: node1/node.hs node2/node.hs node1/WearableExample.hs node2/WearableExample.hs \
	node1/session1.csv

node1/WearableExample.hs: WearableExample.hs
	cp WearableExample.hs node1

node2/WearableExample.hs: WearableExample.hs
	cp WearableExample.hs node2

node2 node1/node.hs node2/node.hs: Main
	./Main

Main: Main.hs
	$(GHC) -i../../src Main.hs

bench: session1.csv
	$(GHC_PREFIX) runghc Criterion.hs -- --output Criterion.html $(bmark)

session1.csv:
	$(GHC_PREFIX) runhaskell ./WearableStreams.hs

node1/session1.csv: session1.csv
	cp $< $@

clean:
	rm -f \
	    Main.hi Main.o \
	    Main.dyn_hi Main.dyn_o \
	    node1/node.hs node2/node.hs \
	    node1/node.hi node2/node.hi \
	    node1/node node2/node \
	    node1/node.o node2/node.o \
	    node1/session1.csv \
	    node1/WearableExample.hs node2/WearableExample.hs \
	    node1/Dockerfile node2/Dockerfile \
	    compose.yml \
	    Criterion.html \
	    session1.csv \
	
	-rmdir node1 node2

.PHONY: clean nodes gen bench
