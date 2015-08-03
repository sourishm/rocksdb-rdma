#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <sched.h>

#include "libxio.h"

#include "../common/rdb_rdma_common.h"
#include "../common/xio_intf.h"

// choose "rdma" or "tcp"
#if defined(_TRANSPORT_TCP)
#define TRANSPORT "tcp"
#else
#define TRANSPORT "rdma"
#endif

#define BILLION             1000000000L
#define HCA_NAME            "ib0"
#define POLLING_TIMEOUT     25 /* 25 microseconds */

struct io_worker_data {
    struct xio_session      *session;
    struct xio_context      *ctx;
    struct xio_connection   *conn;
    struct xio_reg_mem		reg_mem;
    struct xio_msg          msg;
    pthread_t               thread_id;
    int                     affinity;
    int                     run_max;
    uint32_t                t_cnt;
    uint32_t                run_count;
    uint8_t                 command;
};

struct session_data {
    int                     nworkers;
    struct xio_session      *session;
    struct io_worker_data   wdata[MAX_WORKERS];
};

static uint8_t payload_version = RDB_RDMA_PAYLOAD_VERSION;
static uint8_t payload_cmd_put = RDB_CMD_PUT;
static uint8_t payload_cmd_get = RDB_CMD_GET;

static int unique = 1;
static int no_unique = 0;

static int key_size;
static int value_size;
static int multi_ops;
static char *key_data;
static char *value_data;

static void rdb_r_c_run(struct io_worker_data *wdata);

static void make_msg(void *ptr, const uint8_t *payload_cmd, const int *make_unique,
            const uint32_t *t_cnt, const uint32_t *loop_count)
{
    switch (*payload_cmd) {
        case RDB_CMD_PUT:
            {

                struct __attribute__((__packed__)) rdb_put_payload_req {
                    struct rdb_req_hdr          hdr;
                    struct rdb_put_req_hdr      cmd_hdr;
                    struct rdb_key {
                        struct  rdb_key_hdr     key_hdr;
                        char                    key_data[key_size];
                    } key;
                    struct rdb_value {
                        struct  rdb_value_hdr   value_hdr;
                        char                    value_data[value_size];
                    } value;
                } *put_req;

                put_req = ptr;

                if (!(*make_unique)) {
                    put_req->hdr.rdb_version = payload_version;
                    put_req->hdr.rdb_command = payload_cmd_put;
                    put_req->cmd_hdr.key_size = key_size;
                    put_req->cmd_hdr.value_size = value_size;
                    memcpy(put_req->key.key_data, key_data, key_size);
                    memcpy(put_req->value.value_data, value_data, value_size);
                } else {
                    put_req->key.key_hdr.thread_count = *t_cnt;
                    put_req->key.key_hdr.loop_count = *loop_count;
                    put_req->value.value_hdr.thread_count = *t_cnt;
                    put_req->value.value_hdr.loop_count = *loop_count;
                }
                break;
            }
        case RDB_CMD_MPUT:
            {

                int i;

                struct __attribute__((__packed__)) rdb_mput_payload_req {
                    struct rdb_req_hdr              hdr;
                    struct rdb_mput_req_hdr         cmd_hdr;
                    struct multi_kv_pairs {
                        struct rdb_key {
                            struct  rdb_key_hdr     key_hdr;
                            char                    key_data[key_size];
                        } key;
                        struct rdb_value {
                            struct  rdb_value_hdr   value_hdr;
                            char                    value_data[value_size];
                        } value;
                    } m_kv_pairs[multi_ops];
                } *mput_req;

                mput_req = ptr;

                if (!(*make_unique)) {
                    mput_req->hdr.rdb_version = payload_version;
                    mput_req->hdr.rdb_command = payload_cmd_put;
                    mput_req->cmd_hdr.key_size = key_size;
                    mput_req->cmd_hdr.value_size = value_size;
                    mput_req->cmd_hdr.num_records = multi_ops;
                    for (i = 0; i < multi_ops; i++) {
                        memcpy(mput_req->m_kv_pairs[i].key.key_data, key_data, key_size);
                        memcpy(mput_req->m_kv_pairs[i].value.value_data, value_data, value_size);
                    }
                } else {
                    for (i = 0; i < multi_ops; i++) {
                        mput_req->m_kv_pairs[i].key.key_hdr.thread_count = *t_cnt;
                        mput_req->m_kv_pairs[i].key.key_hdr.loop_count = *loop_count + i;
                        mput_req->m_kv_pairs[i].value.value_hdr.thread_count = *t_cnt;
                        mput_req->m_kv_pairs[i].value.value_hdr.loop_count = *loop_count + i;
                    }
                }
                break;
            }
        case RDB_CMD_GET:
            {

                struct __attribute__((__packed__)) rdb_get_payload_req {
                    struct rdb_req_hdr          hdr;
                    struct rdb_get_req_hdr      cmd_hdr;
                    struct rdb_key {
                        struct  rdb_key_hdr     key_hdr;
                        char                    key_data[key_size];
                    } key;
                } *get_req;

                get_req = ptr;

                if (!(*make_unique)) {
                    get_req->hdr.rdb_version = payload_version;
                    get_req->hdr.rdb_command = payload_cmd_get;
                    get_req->cmd_hdr.key_size = key_size;
                    memcpy(get_req->key.key_data, key_data, key_size);
                } else {
                    get_req->key.key_hdr.thread_count = *t_cnt;
                    get_req->key.key_hdr.loop_count = *loop_count;
                }
                break;
            }
        case RDB_CMD_MGET:
            {

                int i;

                struct __attribute__((__packed__)) rdb_mget_payload_req {
                    struct rdb_req_hdr              hdr;
                    struct rdb_mget_req_hdr         cmd_hdr;
                    struct multi_k_pairs {
                        struct rdb_key {
                            struct  rdb_key_hdr     key_hdr;
                            char                    key_data[key_size];
                        } key;
                    } m_k_pairs[multi_ops];
                } *mget_req;

                mget_req = ptr;

                if (!(*make_unique)) {
                    mget_req->hdr.rdb_version = payload_version;
                    mget_req->hdr.rdb_command = payload_cmd_get;
                    mget_req->cmd_hdr.key_size = key_size;
                    mget_req->cmd_hdr.num_records = multi_ops;
                    for (i = 0; i < multi_ops; i++) {
                        memcpy(mget_req->m_k_pairs[i].key.key_data, key_data, key_size);
                    }
                } else {
                    for (i = 0; i < multi_ops; i++) {
                        mget_req->m_k_pairs[i].key.key_hdr.thread_count = *t_cnt;
                        mget_req->m_k_pairs[i].key.key_hdr.loop_count = *loop_count + i;
                    }
                }
                break;
            }
        default:
            break;
    };
}

static int on_session_event(struct xio_session *session,
            struct xio_session_event_data *event_data,
            void *cb_user_context)
{
    struct session_data *session_data;
    int i;

    session_data = cb_user_context;

    printf("%s. reason: %s\n",
            xio_session_event_str(event_data->event),
            xio_strerror(event_data->reason));

    switch (event_data->event) {
        case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
            xio_connection_destroy(event_data->conn);
            break;
        case XIO_SESSION_TEARDOWN_EVENT:
            for (i = 0; i < session_data->nworkers; i++) {
                xio_context_stop_loop(session_data->wdata[i].ctx);
            }
            break;
        default:
            break;
    };

    return (0);
}

static int on_response(struct xio_session *session,
            struct xio_msg *rsp,
            int last_in_rxq,
            void *cb_user_context)
{
    struct io_worker_data *wdata;
    struct xio_iovec_ex *in_sglist;
    struct xio_iovec_ex *out_sglist;
    struct rdb_req_hdr *req_hdr;

    wdata = cb_user_context;

    out_sglist = vmsg_sglist(&rsp->out);
    req_hdr = out_sglist[0].iov_base;

    //printf("response : ver %d\n", req_hdr->rdb_version);

    switch (req_hdr->rdb_command) {
        case RDB_CMD_PUT:
        case RDB_CMD_MPUT:
            //printf("response : cmd %d\n", req_hdr->rdb_command);

            break;
        case RDB_CMD_GET:
            {
//                struct rdb_value_hdr *v_hdr;
                int *status;

                in_sglist = vmsg_sglist(&rsp->in);
                status = in_sglist[0].iov_base;
                if (!*status) {
                    fprintf(stderr, "GET command failed\n");
                    break;
                }
//                v_hdr = in_sglist[0].iov_base + sizeof (*status);
                /*printf("response : get : value %d:%d\n", v_hdr->thread_count,
                  v_hdr->loop_count);*/

                break;
            }
        case RDB_CMD_MGET:
            {
                int *status;

                in_sglist = vmsg_sglist(&rsp->in);
                status = in_sglist[0].iov_base;
                if (!*status) {
                    fprintf(stderr, "GET command failed\n");
                }

                //printf("response : mget : status %d\n", *status);

                break;
            }
        default:
            break;
    }

    xio_release_response(rsp);

    rdb_r_c_run(wdata);

    return (0);
}

/* Session Callbacks */
static struct xio_session_ops ses_ops = {
    .on_session_event           =  on_session_event,
    .on_session_established     =  NULL,
    .on_ow_msg_send_complete    =  NULL,
    .on_msg                     =  on_response,
    .on_msg_error               =  NULL
};

/* Init the accelio lib and create session */
struct session_data* rdb_r_c_open(const char *host, const int *port)
{
    struct xio_session_params params = {0};
    struct session_data *session_data;
    char url[NAME_LEN];
    int error;

    /* init the accelio library */
    xio_init();

    sprintf(url, "%s://%s:%d", TRANSPORT, host, *port);

    session_data = (struct session_data *) calloc(1, sizeof(*session_data));
    if (!session_data) {
        fprintf(stderr, "failed to allocate memory for session_data\n");
        return (NULL);
    }

    params.type = XIO_SESSION_CLIENT;
    params.ses_ops = &ses_ops;
    params.user_context = session_data;
    params.uri = url;

    /* create accelio session */
    session_data->session = xio_session_create(&params);
    if (!session_data->session) {
        error = xio_errno();
        fprintf(stderr, "failed to create session. reason %d - (%s)\n",
                error, xio_strerror(error));
        free(session_data);
        return (NULL);
    }

    return (session_data);
}

/* Destroy the session and shutdown the accelio lib */
void rdb_r_c_close(struct session_data *session_data)
{
    int error;

    /* close the session */
    if (session_data->session) {
        error = xio_session_destroy(session_data->session);
        if (error) {
            error = xio_errno();
            fprintf(stderr, "failed to destroy session. reason %d - (%s)\n",
                    error, xio_strerror(error));
        }
    }

    if (session_data)
        free(session_data);

    xio_shutdown();
}

/*
 * 1. Create thread context and connection
 * 2. Register memory buffer for transfer
 */
int rdb_r_c_connect(struct io_worker_data *wdata)
{
    void *ptr;
    struct xio_connection_params cparams = {0};
    int buf_len;
    int error = 0;

    /* create thread context */
    wdata->ctx = xio_context_create(NULL, POLLING_TIMEOUT,
                                    wdata->affinity);
    if (!wdata->ctx) {
        error = xio_errno();
        fprintf(stderr, "failed to create context for thread %d. reason %d - (%s)\n",
                wdata->t_cnt, error, xio_strerror(error));
        return (error);
    }

    cparams.session = wdata->session;
    cparams.ctx = wdata->ctx;
    cparams.conn_user_context = wdata;

    /* connect the session */
    wdata->conn = xio_connect(&cparams);
    if (!wdata->conn) {
        error = xio_errno();
        fprintf(stderr, "failed to connect for thread %d. reason %d - (%s)\n",
                wdata->t_cnt, error, xio_strerror(error));
        return (error);
    }

    /* allocate memory for rdma transfer */
    switch (wdata->command) {
        case RDB_CMD_PUT:
            buf_len = RDB_PUT_PAYLOAD_REQ_LEN + key_size + value_size;
            break;
        case RDB_CMD_GET:
            buf_len = RDB_GET_PAYLOAD_REQ_LEN + key_size;
            break;
        case RDB_CMD_MPUT:
            buf_len = RDB_MPUT_PAYLOAD_REQ_LEN + multi_ops * (key_size +
                        value_size + sizeof(struct rdb_key_hdr) +
                        sizeof(struct rdb_value_hdr));
            break;
        case RDB_CMD_MGET:
            buf_len = RDB_MGET_PAYLOAD_REQ_LEN + multi_ops * (key_size +
                        sizeof(struct rdb_key_hdr));
            break;
        default:
            break;
    };
    xio_mem_alloc(buf_len, &wdata->reg_mem);
    if (!wdata->reg_mem.addr) {
        error = xio_errno();
        fprintf(stderr, "failed to allocate accelio transfer buffer for thread %d. reason %d - (%s)\n",
                wdata->t_cnt, error, xio_strerror(error));
        return (error);
    }
    ptr = wdata->reg_mem.addr;
    make_msg(ptr, &wdata->command, &no_unique,
            &wdata->t_cnt, &wdata->run_count);

    return (error);
}

void rdb_r_c_disconnect(struct io_worker_data *wdata)
{
    if (wdata->reg_mem.addr)
        xio_mem_free(&wdata->reg_mem);
    if (wdata->conn)
        xio_disconnect(wdata->conn);
    if (wdata->ctx)
        xio_context_destroy(wdata->ctx);
}

/* Send data to rdb server for PUT operation */
static void rdb_r_c_put(struct io_worker_data *wdata)
{
    struct xio_msg *msg;
    struct xio_iovec_ex *sglist;
    void *ptr;

    msg = &wdata->msg;

    msg->in.header.iov_len = 0;
    sglist = vmsg_sglist(&msg->in);
    vmsg_sglist_set_nents(&msg->in, 0);

    msg->out.header.iov_len = 0;
    sglist = vmsg_sglist(&msg->out);
    ptr = wdata->reg_mem.addr;

    make_msg(ptr, &payload_cmd_put, &unique,
            &wdata->t_cnt, &wdata->run_count);

    sglist[0].iov_base  = wdata->reg_mem.addr;
    sglist[0].iov_len   = wdata->reg_mem.length;
    sglist[0].mr        = wdata->reg_mem.mr;
    vmsg_sglist_set_nents(&msg->out, 1);

    /*printf("payload_cmd %d, payload_version %d, key %s, value %s, len %d, thread count %d\n",
            payload_cmd_put,
            payload_version,
            key,
            value,
            (int)sglist[0].iov_len,
            (int)wdata->t_cnt);*/

    /* send message */
    if (xio_send_request(wdata->conn, msg) == -1) {
        fprintf(stderr, "failed PUT request for thread %d, loop count %d. reason %d - (%s)\n",
                wdata->t_cnt, wdata->run_count, xio_errno(), xio_strerror(xio_errno()));
    }
}

/* Send data to rdb server for GET operation */
static void rdb_r_c_get(struct io_worker_data *wdata)
{
    struct xio_msg *msg;
    struct xio_iovec_ex *sglist;
    void *ptr;

    msg = &wdata->msg;

    msg->in.header.iov_len = 0;
    sglist = vmsg_sglist(&msg->in);
    vmsg_sglist_set_nents(&msg->in, 0);

    msg->out.header.iov_len = 0;
    sglist = vmsg_sglist(&msg->out);
    ptr = wdata->reg_mem.addr;

    make_msg(ptr, &payload_cmd_get, &unique,
            &wdata->t_cnt, &wdata->run_count);

    sglist[0].iov_base  = wdata->reg_mem.addr;
    sglist[0].iov_len   = wdata->reg_mem.length;
    sglist[0].mr        = wdata->reg_mem.mr;
    vmsg_sglist_set_nents(&msg->out, 1);

    /*printf("payload_cmd %d, payload_version %d, key %s, len %d, thread count %d\n",
            payload_cmd_get,
            payload_version,
            key,
            (int)sglist[0].iov_len,
            (int)wdata->t_cnt);*/

    /* send message */
    if (xio_send_request(wdata->conn, msg) == -1) {
        fprintf(stderr, "failed GET request for thread %d, loop count %d. reason %d - (%s)\n",
                wdata->t_cnt, wdata->run_count, xio_errno(), xio_strerror(xio_errno()));
    }
}

static void rdb_r_c_run(struct io_worker_data *wdata)
{
    if (wdata->run_count < wdata->run_max) {
        switch (wdata->command) {
            case RDB_CMD_PUT:
            case RDB_CMD_MPUT:
                rdb_r_c_put(wdata);
                break;
            case RDB_CMD_GET:
            case RDB_CMD_MGET:
                rdb_r_c_get(wdata);
                break;
            default:
                break;
        };
        if ((wdata->command == RDB_CMD_MPUT) ||
                (wdata->command == RDB_CMD_MGET))
            wdata->run_count += multi_ops;
        else
            wdata->run_count++;
    } else {
        xio_context_stop_loop(wdata->ctx);
    }
}

/* Client I/O manager */
void *rdb_r_c_io_worker(void *data)
{
    struct io_worker_data *wdata;
    cpu_set_t cpuset;
    struct timespec start_time, end_time;
    uint64_t time_diff;
    int error;

    wdata = data;

    /* set affinity to thread */
    CPU_ZERO(&cpuset);
    CPU_SET(wdata->affinity, &cpuset);
    pthread_setaffinity_np(wdata->thread_id, sizeof(cpu_set_t), &cpuset);

    error = rdb_r_c_connect(wdata);
    if (error)
        goto disconnect;
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    rdb_r_c_run(wdata);
    error = xio_context_run_loop(wdata->ctx, XIO_INFINITE);
    if (error) {
        error = xio_errno();
        fprintf(stderr, "failed to run event loop for thread %d. reason %d - (%s)\n",
                wdata->t_cnt, error, xio_strerror(error));
    }
    clock_gettime(CLOCK_MONOTONIC, &end_time);

    time_diff = BILLION * (end_time.tv_sec - start_time.tv_sec) +
                (end_time.tv_nsec - start_time.tv_nsec);
    printf("elapsed time for thread with thread_count %d is %lld nanoseconds\n",
            wdata->t_cnt, (long long unsigned int) time_diff);
    printf("run count for thread with thread_count %d is %d\n",
            wdata->t_cnt, wdata->run_count);
    printf("average latency for thread with thread_count %d is % lld nanoseconds\n",
            wdata->t_cnt, (long long unsigned int) (time_diff/wdata->run_count));

disconnect:
    rdb_r_c_disconnect(wdata);
    return NULL;
}

/* Create io_worker threads */
void rdb_r_c_create_workers(struct session_data *session_data,
                            int num_threads, int runs, int cmd)
{
    uint64_t cpusmask;
    int max_cpus;
    int cpusnr;
    int cpu;
    int i;

    num_threads = (num_threads <= MAX_WORKERS) ? num_threads : MAX_WORKERS;

    max_cpus    = sysconf(_SC_NPROCESSORS_ONLN);
    (void) intf_name_best_cpus(HCA_NAME, &cpusmask, &cpusnr);

    for (i = 0, cpu = 0; i < num_threads; i++, cpu++) {
        /*while (1) {
            if (cpusmask_test_bit(cpu, &cpusmask))
                break;
            if (++cpu == max_cpus)
                cpu = 0;
        }*/
        if (++cpu == max_cpus)
            cpu = 0;
        /* all threads will work on the same session */
        session_data->wdata[i].affinity = cpu;
        session_data->wdata[i].session = session_data->session;
        session_data->nworkers++;
        session_data->wdata[i].t_cnt = i;
        session_data->wdata[i].run_max = runs;
        session_data->wdata[i].command = (uint8_t) cmd;
        pthread_create(&session_data->wdata[i].thread_id, NULL,
                rdb_r_c_io_worker, &session_data->wdata[i]);
        printf("client thread created with thread count %d\n",
                (int)session_data->wdata[i].t_cnt);
    }

    /* join the threads */
    for (i = 0; i < num_threads; i++) {
        pthread_join(session_data->wdata[i].thread_id, NULL);
    }
}

static void usage()
{
    printf("Usage:\n");
    printf("\t-i    host address of server to connect\n");
    printf("\t-p    port\n");
    printf("\t-t    number of threads\n");
    printf("\t-o    number of operations\n");
    printf("\t-k    key size in bytes\n");
    printf("\t-v    value size in bytes\n");
    printf("\t-m    multi get/put batch count, number of records per operation\n");
    printf("\t-c    command\n");
    printf("\t\t    1. PUT\n");
    printf("\t\t    2. GET\n");
    printf("\t\t    3. MPUT\n");
    printf("\t\t    4. MGET\n");
    printf("\t-h    help\n");
}

int main(int argc, char *argv[])
{
    struct session_data *session_data;
    char host[NAME_LEN];
    int port;
    int cmd;
    int n_threads;
    int n_ops;
    int c;
    int min_options = 0;

    n_threads = 1;
    n_ops = 1000;
    key_size = 16;
    value_size = 256;
    multi_ops = 2;
    cmd = RDB_CMD_PUT;

    opterr=0;
    while ((c = getopt(argc, argv, "i:p:t:o:k:v:m:c:h")) != -1) {
        switch (c) {
            case 'i':
                sprintf(host, "%s", optarg);
                min_options++;
                break;
            case 'p':
                port = atoi(optarg);
                min_options++;
                break;
            case 't':
                n_threads = atoi(optarg);
                break;
            case 'o':
                n_ops = atoi(optarg);
                break;
            case 'k':
                key_size = atoi(optarg);
                break;
            case 'v':
                value_size = atoi(optarg);
                break;
            case 'm':
                multi_ops = atoi(optarg);
            case 'c':
                cmd = atoi(optarg);
                break;
            case 'h':
                usage();
                exit(0);
                break;
            default:
                fprintf(stderr, "invalid command option\n");
                usage();
                exit(1);
        };
    }
    if (min_options < 2) {
        fprintf(stderr, "host address and port are required\n");
        usage();
        exit(1);
    }

    key_data = malloc(key_size);
    value_data = malloc(value_size);
    memset(key_data, 'a', key_size);
    memset(value_data, 'b', value_size);

    session_data = rdb_r_c_open(host, &port);
    if (!session_data)
        exit(1);

    rdb_r_c_create_workers(session_data, n_threads, n_ops, cmd);

    rdb_r_c_close(session_data);

    return 0;
}
