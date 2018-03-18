.PHONY: all clean test

BUILD=jbuilder build --dev
CLEAN= jbuilder clean
TEST=jbuilder runtest -j1 --no-buffer --dev

all:
		${BUILD}

test:
		${TEST}

clean:
	${CLEAN}
