CC=gcc
CPP=g++
CFLAGS=-g -O2
DFLAGS=-D_GNU_SOURCE
ROCKSDB_DFLAGS=-DROCKSDB_PLATFORM_POSIX -DOS_LINUX -DROCKSDB_FALLOCATE_PRESENT -DSNAPPY -DGFLAGS=google -DZLIB -DBZIP2
LDFLAGS=-lrdmacm -libverbs -lpthread -lxio -lnuma -lrt
ROCKSDB_LDFLAGS=-lpthread -lrt -lsnappy -lgflags -lz -lbz2
IFLAGS=-I /usr/local/include/
COMMON=../common/

rdb_rdma_server: rdb_rdma_server.o $(COMMON)xio_intf.o rocksdb_server.o
					$(CPP) -o rdb_rdma_server rdb_rdma_server.o $(COMMON)xio_intf.o rocksdb_server.o /usr/local/lib/librocksdb.a $(IFLAGS) -std=c++11 $(CFLAGS) $(DFLAGS) $(LDFLAGS) $(ROCKSDB_DFLAGS) $(ROCKSDB_LDFLAGS)

rdb_rdma_server.o: $(COMMON)xio_intf.c rdb_rdma_server.c rdb_server.h $(COMMON)xio_intf.h $(COMMON)rdb_rdma_common.h
					$(CC) -c $(COMMON)xio_intf.c rdb_rdma_server.c $(CFLAGS) $(DFLAGS) $(LDFLAGS)

rocksdb_server.o: rocksdb_server.cpp rocksdb_server.h rdb_server.h
					$(CPP) -c rocksdb_server.cpp /usr/local/lib/librocksdb.a $(IFLAGS) -std=c++11 -fno-builtin-memcmp -march=native $(CFLAGS) $(ROCKSDB_DFLAGS) $(ROCKSDB_LDFLAGS)

clean:
	-rm -rf rdb_rdma_server
	-rm -rf *.o
	-rm -rf $(COMMON)*.o
