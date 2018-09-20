.PHONY: all clean test doc

BUILD_LIB=dune build
BUILD_BROKER=dune build src/zenoh-broker/zenohd.exe src/zenoh-broker/zenohb.exe
BUILD_CLIENT=dune build src/zenoh-broker/zenohc.exe
BUILD_API_EXAMPLE_SUB=dune build example/zenoh-api/sub.exe
BUILD_API_EXAMPLE_PUB=dune build example/zenoh-api/pub.exe
BUILD_ROUNDTRIP_PING=dune build example/roundtrip/roundtrip_ping.exe
BUILD_ROUNDTRIP_PONG=dune build example/roundtrip/roundtrip_pong.exe
BUILD_THROUGHPUT_SUB=dune build example/throughput/throughput_sub.exe
BUILD_THROUGHPUT_PUB=dune build example/throughput/throughput_pub.exe
CLEAN= dune clean
TEST=dune runtest -j1 --no-buffer
DOC=dune build @doc
INSTALL=dune install

all:
		${BUILD_LIB}
		${BUILD_BROKER}
		${BUILD_CLIENT}
		${BUILD_API_EXAMPLE_SUB}
		${BUILD_API_EXAMPLE_PUB}
		${BUILD_ROUNDTRIP_PING}
		${BUILD_ROUNDTRIP_PONG}
		${BUILD_THROUGHPUT_SUB}
		${BUILD_THROUGHPUT_PUB}
		
test:
		${TEST}

doc:
	${DOC}

install:
		${INSTALL}

clean:
	${CLEAN}
