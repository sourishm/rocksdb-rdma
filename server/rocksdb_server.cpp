#include <cstdio>
#include <iostream>
#include <vector>
#include "rocksdb_server.h"
#include "rdb_server.h"

#include <assert.h>
#include <memory.h>

const string db_path = "/tmp/rocksdb_example";

rocksdb_server::rocksdb_server()
{
    Options options;
    Status s;

    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    options.compression = kNoCompression;

    s = DB::Open(options, db_path, &db);
    assert(s.ok());
}

rocksdb_server::~rocksdb_server()
{
    delete db;
}

int rocksdb_server::rdb_put(const char *key, const size_t *key_size,
        const char *value, const size_t *value_size)
{
    Status s;

    s = db->Put(WriteOptions(), Slice(key, *key_size),
            Slice(value, *value_size));
    return (s.ok());
}

int rocksdb_server::rdb_mput(const char *records, const size_t *key_size,
        const size_t *value_size, const int *num_records)
{
    Status s;
    WriteBatch batch;
    int i;

    for (i = 0; i < *num_records; i++) {
        batch.Put(Slice(records, *key_size),
                Slice((records + *key_size), *value_size));
        records += *key_size;
        records += *value_size;
    }

    s = db->Write(WriteOptions(), &batch);

    return (s.ok());
}

int rocksdb_server::rdb_get(const char *key, const size_t *key_size,
        char *value, size_t *value_size)
{
    Status s;
    string val;

    s = db->Get(ReadOptions(), Slice(key, *key_size), &val);
    if (s.ok()) {
        *value_size = val.size();
        memcpy(value, val.c_str(), *value_size);
    }

    return (s.ok());
}

int rocksdb_server::rdb_mget(const char *records, const size_t *key_size,
        char *value, size_t *value_size,
        const int *num_records)
{
    Status s;
    string val;
    size_t val_size;
    int i;
    int nops = 0;

    *value_size = 0;

    for (i = 0; i < *num_records; i++) {
        s = db->Get(ReadOptions(), Slice(records, *key_size),
                &val);
        records += *key_size;
        if (s.ok()) {
            val_size = val.size();
            memcpy(value, val.c_str(), val_size);
            value += val_size;
            *value_size += val_size;
            nops++;
        }
    }

    return (nops);
}


extern "C" void* new_rocksdb_server()
{
    rocksdb_server *rdb_s = new rocksdb_server();

    return (rdb_s);
}

extern "C" void delete_rocksdb_server(void *data)
{
    rocksdb_server *rdb_s;

    if (data) {
        rdb_s = (rocksdb_server *) data;
        delete rdb_s;
    }
}

extern "C" int rocksdb_server_put(void *data, const char *key, const size_t *key_size,
        const char *value, const size_t *value_size)
{
    rocksdb_server *rdb_s;

    if (data) {
        rdb_s = (rocksdb_server *) data;
        return rdb_s->rdb_put(key, key_size, value, value_size);
    }

    return (0);
}

extern "C" int rocksdb_server_mput(void *data, const char *records, const size_t *key_size,
        const size_t *value_size, const int *num_records)
{
    rocksdb_server *rdb_s;

    if (data) {
        rdb_s = (rocksdb_server *) data;
        return rdb_s->rdb_mput(records, key_size, value_size, num_records);
    }

    return (0);
}

extern "C" int rocksdb_server_get(void *data, const char *key, const size_t *key_size,
        char *value, size_t *value_size)
{
    rocksdb_server *rdb_s;

    if (data) {
        rdb_s = (rocksdb_server *) data;
        return rdb_s->rdb_get(key, key_size, value, value_size);
    }

    return (0);
}

extern "C" int rocksdb_server_mget(void *data, const char *records, const size_t *key_size,
        char *value, size_t *value_size, const int *num_records)
{
    rocksdb_server *rdb_s;

    if (data) {
        rdb_s = (rocksdb_server *) data;
        return rdb_s->rdb_mget(records, key_size, value, value_size, num_records);
    }

    return (0);
}
