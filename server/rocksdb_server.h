#ifndef H_ROCKSDB_SERVER
#define H_ROCKSDB_SERVER

#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;
using namespace std;

class rocksdb_server {
    private:
        DB  *db;
    public:
        rocksdb_server();
        ~rocksdb_server();
        int rdb_put(const char *key, const size_t *key_size,
                        const char *value, const size_t *value_size);
        int rdb_mput(const char *records, const size_t *key_size,
                        const size_t *value_size, const int *num_records);
        int rdb_get(const char *key, const size_t *key_size,
                        char *value, size_t *value_size);
        int rdb_mget(const char *records, const size_t *key_size,
                        char *value, size_t *value_size,
                        const int *num_records);
};

#endif
