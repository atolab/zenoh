.PHONY: all clean test doc

BUILD=dune build
RELEASE=dune build --profile release 
CLEAN= dune clean
TEST=dune runtest -j1 --no-buffer
DOC=dune build @doc
INSTALL=dune install
UNINSTALL=dune uninstall

BROKER=src/zenoh-broker/zenohd.exe
CLIENT=src/zenoh-cat/zenohc.exe
API_EXAMPLE_SUB=example/zenoh-api/sub.exe
API_EXAMPLE_PUB=example/zenoh-api/pub.exe
API_EXAMPLE_STO=example/zenoh-api/storage.exe
API_EXAMPLE_QUE=example/zenoh-api/query.exe
ROUNDTRIP_PING=example/roundtrip/roundtrip_ping.exe
ROUNDTRIP_PONG=example/roundtrip/roundtrip_pong.exe
THROUGHPUT_SUB=example/throughput/throughput_sub.exe
THROUGHPUT_PUB=example/throughput/throughput_pub.exe

all:
	${BUILD} 
	${BUILD} ${BROKER}
	${BUILD} ${CLIENT}
	${BUILD} ${API_EXAMPLE_SUB}
	${BUILD} ${API_EXAMPLE_PUB}
	${BUILD} ${API_EXAMPLE_STO}
	${BUILD} ${API_EXAMPLE_QUE}
	${BUILD} ${ROUNDTRIP_PING}
	${BUILD} ${ROUNDTRIP_PONG}
	${BUILD} ${THROUGHPUT_SUB}
	${BUILD} ${THROUGHPUT_PUB}

release:
	${RELEASE} 
	${RELEASE} ${BROKER}
	${RELEASE} ${CLIENT}
	${RELEASE} ${API_EXAMPLE_SUB}
	${RELEASE} ${API_EXAMPLE_PUB}
	${RELEASE} ${API_EXAMPLE_STO}
	${RELEASE} ${API_EXAMPLE_QUE}
	${RELEASE} ${ROUNDTRIP_PING}
	${RELEASE} ${ROUNDTRIP_PONG}
	${RELEASE} ${THROUGHPUT_SUB}
	${RELEASE} ${THROUGHPUT_PUB}
	
test:
	${TEST}

doc:
	${DOC}

install:
	${INSTALL}

clean:
	${CLEAN}

uninstall:
	${UNINSTALL}
