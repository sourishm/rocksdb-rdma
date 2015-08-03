CC=gcc
CFLAGS=-g -O2
DFLAGS=-D_GNU_SOURCE
LDFLAGS=-lrdmacm -libverbs -lpthread -lxio -lnuma -lrt
COMMON=../common/

rdb_rdma_client: $(COMMON)xio_intf.c rdb_rdma_client.c $(COMMON)xio_intf.h $(COMMON)rdb_rdma_common.h
					$(CC) -o rdb_rdma_client $(COMMON)xio_intf.c rdb_rdma_client.c $(CFLAGS) $(DFLAGS) $(LDFLAGS)

clean:
	-rm -rf rdb_rdma_client
	-rm -rf *.o
	-rm -rf $(COMMON)*.o
