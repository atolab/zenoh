.PHONY: all clean test doc

BUILD=jbuilder build #--dev
CLEAN= jbuilder clean
TEST=jbuilder runtest -j1 --no-buffer #--dev
DOC=jbuilder build --dev @doc
INSTALL=jbuilder install

all:
		${BUILD}

test:
		${TEST}

doc:
	${DOC}

install:
		${INSTALL}

clean:
	${CLEAN}
