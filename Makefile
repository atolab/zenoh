.PHONY: all clean test doc

BUILD_LIB=dune build --profile=release
BUILD_BROKER=dune build src/zenoh-broker/zenohd.exe --profile=release
BUILD_CLIENT=dune build src/zenoh-broker/zenohc.exe --profile=release
BUILD_API_EXAMPLE_SUB=dune build example/zenoh-api/sub.exe --profile=release
BUILD_API_EXAMPLE_PUB=dune build example/zenoh-api/pub.exe --profile=release
BUILD_ROUNDTRIP_PING=dune build example/roundtrip/roundtrip_ping.exe --profile=release
BUILD_ROUNDTRIP_PONG=dune build example/roundtrip/roundtrip_pong.exe --profile=release
CLEAN= dune clean
TEST=dune runtest -j1 --no-buffer
DOC=dune build --dev @doc
INSTALL=dune install

all:
		${BUILD_LIB}
		${BUILD_BROKER}
		${BUILD_CLIENT}
		${BUILD_API_EXAMPLE_SUB}
		${BUILD_API_EXAMPLE_PUB}
		${BUILD_ROUNDTRIP_PING}
		${BUILD_ROUNDTRIP_PONG}
		
test:
		${TEST}

doc:
	${DOC}

install:
		${INSTALL}

clean:
	${CLEAN}
