prefix = striot

base = striot-base/striot
license = $(base)/LICENSE
readme = $(base)/README.md
cabal = $(base)/striot.cabal

src_dir := $(base)/src
in_srcs := $(wildcard ../src/*.hs) $(wildcard ../src/**/*.hs)
out_srcs := $(subst ..,$(base),$(in_srcs))

.PHONY: all
all: $(cabal) $(license) $(readme) $(out_srcs)
	@echo "Building image $(prefix)/striot-base"
	@docker build -t $(prefix)/striot-base striot-base

$(out_srcs): $(src_dir) $(in_srcs)
	@cp -R ../src/* $(src_dir)

$(cabal): $(src_dir) ../striot.cabal
	@cp -R ../striot.cabal $(base)/

$(license): $(src_dir) ../LICENSE
	@cp -R ../LICENSE $(base)/

$(readme): $(src_dir) ../README.md
	@cp -R ../README.md $(base)/

$(src_dir):
	@mkdir -p $(src_dir)

.PHONY: clean
clean:
	@echo "Removing striot-base/striot"
	@rm -rf striot-base/striot
	@if [ -n "$$(docker images -q $(prefix)/striot-base)" ]; then \
		docker rmi $(prefix)/striot-base; \
	fi
