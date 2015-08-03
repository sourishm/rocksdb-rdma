#ifndef H_RDB_RDMA_COMMON
#define H_RDB_RDMA_COMMON

#define     NAME_LEN            256
#define     MAX_WORKERS         16

#define     RDB_RDMA_PAYLOAD_VERSION    1
#define     RDB_CMD_PUT                 1
#define     RDB_CMD_GET                 2
#define     RDB_CMD_MPUT                3
#define     RDB_CMD_MGET                4

struct rdb_key_hdr {
    uint32_t    thread_count;
    uint32_t    loop_count;
};

struct rdb_value_hdr {
    uint32_t    thread_count;
    uint32_t    loop_count;
};

struct rdb_req_hdr {
    uint8_t     rdb_version;
    uint8_t     rdb_command;
};

struct rdb_put_req_hdr {
    uint32_t    key_size;
    uint32_t    value_size;
};

struct rdb_mput_req_hdr {
    uint32_t    key_size;
    uint32_t    value_size;
    uint32_t    num_records;
};

struct rdb_get_req_hdr {
    uint32_t    key_size;
};

struct rdb_mget_req_hdr {
    uint32_t    key_size;
    uint32_t    num_records;
};

#define     RDB_PUT_PAYLOAD_REQ_LEN     sizeof(struct rdb_req_hdr)+\
                                        sizeof(struct rdb_put_req_hdr)+\
                                        sizeof(struct rdb_key_hdr)+\
                                        sizeof(struct rdb_value_hdr)

#define     RDB_MPUT_PAYLOAD_REQ_LEN    sizeof(struct rdb_req_hdr)+\
                                        sizeof(struct rdb_mput_req_hdr)

#define     RDB_GET_PAYLOAD_REQ_LEN     sizeof(struct rdb_req_hdr)+\
                                        sizeof(struct rdb_get_req_hdr)+\
                                        sizeof(struct rdb_key_hdr)

#define     RDB_MGET_PAYLOAD_REQ_LEN    sizeof(struct rdb_req_hdr)+\
                                        sizeof(struct rdb_mget_req_hdr)

#endif
