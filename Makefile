.PHONY: all clean test doc

DUNE_BUILD=dune build
DUNE_RELEASE=dune build --profile release 

BROKER=src/zenoh-broker/zenohd.exe
CLIENT=src/zenoh-broker/zenohc.exe
API_EXAMPLE_SUB=example/zenoh-api/sub.exe
API_EXAMPLE_PUB=example/zenoh-api/pub.exe
API_EXAMPLE_STO=example/zenoh-api/storage.exe
API_EXAMPLE_QUE=example/zenoh-api/query.exe
ROUNDTRIP_PING=example/roundtrip/roundtrip_ping.exe
ROUNDTRIP_PONG=example/roundtrip/roundtrip_pong.exe
THROUGHPUT_SUB=example/throughput/throughput_sub.exe
THROUGHPUT_PUB=example/throughput/throughput_pub.exe
CLEAN= dune clean
TEST=dune runtest -j1 --no-buffer
DOC=dune build @doc
INSTALL=dune install

all:
		${DUNE_BUILD} 
		${DUNE_BUILD} ${BROKER}
		${DUNE_BUILD} ${CLIENT}
		${DUNE_BUILD} ${API_EXAMPLE_SUB}
		${DUNE_BUILD} ${API_EXAMPLE_PUB}
		${DUNE_BUILD} ${API_EXAMPLE_STO}
		${DUNE_BUILD} ${API_EXAMPLE_QUE}
		${DUNE_BUILD} ${ROUNDTRIP_PING}
		${DUNE_BUILD} ${ROUNDTRIP_PONG}
		${DUNE_BUILD} ${THROUGHPUT_SUB}
		${DUNE_BUILD} ${THROUGHPUT_PUB}

release:
		${DUNE_RELEASE} 
		${DUNE_RELEASE} ${BROKER}
		${DUNE_RELEASE} ${CLIENT}
		${DUNE_RELEASE} ${API_EXAMPLE_SUB}
		${DUNE_RELEASE} ${API_EXAMPLE_PUB}
		${DUNE_RELEASE} ${API_EXAMPLE_STO}
		${DUNE_RELEASE} ${API_EXAMPLE_QUE}
		${DUNE_RELEASE} ${ROUNDTRIP_PING}
		${DUNE_RELEASE} ${ROUNDTRIP_PONG}
		${DUNE_RELEASE} ${THROUGHPUT_SUB}
		${DUNE_RELEASE} ${THROUGHPUT_PUB}		
test:
		${TEST}

doc:
	${DOC}

install:
		${INSTALL}

clean:
	${CLEAN}
