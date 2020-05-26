debug:
	clang -I ../include -L ../../target/debug/ -lzenoh_ffi session_test.c -o session_test

release:
	clang -O3 -I ../include -L ../../target/release/ -lzenoh_ffi session_test.c -o session_test
	clang -O3 -I ../include -L ../../target/release/ -lzenoh_ffi thrput_test.c -o thrput_test


clean:
	rm thrput_test session_test