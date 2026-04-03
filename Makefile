CFLAGS = -Wall -fPIC -shared -I. -Iupb_out -I/usr/include -msse4.1
LDFLAGS =

all: lite_proto.so

lite_proto.so: lite_proto.c upb_out/upb.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o lite_proto.so lite_proto.c upb_out/*.c

clean:
	rm -f lite_proto.so

coverage: clean
	$(MAKE) lite_proto.so CFLAGS="$(CFLAGS) -fprofile-arcs -ftest-coverage"
	python3 test_vtab.py
	mv lite_proto.so-lite_proto.gcno lite_proto.gcno
	mv lite_proto.so-lite_proto.gcda lite_proto.gcda
	gcov lite_proto.c

leaks: clean
	$(MAKE) lite_proto.so CFLAGS="-Wall -fPIC -shared -I. -Iupb_out -I/usr/include -msse4.1 -g -O0"
	$(CC) -g -O0 -o test_leaks test_leaks.c -lsqlite3 -msse4.1
	valgrind --leak-check=full --undef-value-errors=no --error-exitcode=1 ./test_leaks
