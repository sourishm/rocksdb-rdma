#ifndef H_RDB_SERVER
#define H_RDB_SERVER

#ifdef __cplusplus
extern "C" {
#endif

void* new_rocksdb_server();
void delete_rocksdb_server(void *data);
int rocksdb_server_put(void *data, const char *key, const size_t *key_size,
                        const char *value, const size_t *value_size);
int rocksdb_server_mput(void *data, const char *records, const size_t *key_size,
                        const size_t *value_size, const int *num_records);
int rocksdb_server_get(void *data, const char *key, const size_t *key_size,
                        char *value, size_t *value_size);
int rocksdb_server_mget(void *data, const char *records, const size_t *key_size,
                        char *value, size_t *value_size, const int *num_records);

#ifdef __cplusplus
}
#endif

#endif
