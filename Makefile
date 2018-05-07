.PHONY: all clean test doc

BUILD_LIB=jbuilder build #--dev
BUILD_BROKER=jbuilder build src/zenoh-broker/zenohd.exe
BUILD_CLIENT=jbuilder build src/zenoh-broker/zenohc.exe
CLEAN= jbuilder clean
TEST=jbuilder runtest -j1 --no-buffer #--dev
DOC=jbuilder build --dev @doc
INSTALL=jbuilder install

all:
		${BUILD_LIB}
		${BUILD_BROKER}
		${BUILD_CLIENT}
		
test:
		${TEST}

doc:
	${DOC}

install:
		${INSTALL}

clean:
	${CLEAN}
