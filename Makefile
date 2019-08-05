.PHONY: all release test doc install clean uninstall

all:
	dune build @all

release:
	dune build @all --profile release
	
test:
	dune runtest -j1 --no-buffer

doc:
	dune build @doc

install:
	dune install

clean:
	dune clean

uninstall:
	dune uninstall
