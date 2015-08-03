#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <inttypes.h>
#include <sched.h>

#include "libxio.h"

#include "rdb_server.h"
#include "../common/rdb_rdma_common.h"
#include "../common/xio_intf.h"

// choose "rdma" or "tcp"
#if defined(_TRANSPORT_TCP)
#define TRANSPORT "tcp"
#else
#define TRANSPORT "rdma"
#endif

#define MAX_VALUE_SIZE      8192*8 /* 8KB max value size */
#define POLLING_TIMEOUT     25 /* 25 microseconds */

struct portals_vec {
    int             vec_len;
    const char      *vec[MAX_WORKERS];
};

struct io_worker_data {
    char                    portal[NAME_LEN];
    void                    *rdb; /* rocksdb server instance */
    struct xio_context      *ctx;
    struct xio_connection   *conn;
    struct xio_reg_mem		reg_mem;
    struct xio_msg          rsp;
    pthread_t               thread_id;
    int                     affinity;
};

struct server_data {
    char                    host[NAME_LEN];
    int                     port;
    int                     nworkers;
    struct xio_context      *ctx;
    struct xio_server       *server;
    struct io_worker_data   wdata[MAX_WORKERS];
};

static int null_mode;

static struct portals_vec *portals_get(struct server_data *server_data,
                            const char *uri, void *user_context)
{
    int i;
    struct portals_vec  *portals;

    portals = (struct portals_vec *) calloc(1, sizeof(*portals));

    for (i = 0; i < server_data->nworkers; i++) {
        portals->vec[i] = strdup(server_data->wdata[i].portal);
        portals->vec_len++;
    }

    return portals;
}

static void portals_free(struct portals_vec *portals)
{
    int         i;  
    for (i = 0; i < portals->vec_len; i++)
        free((char *)(portals->vec[i]));

    free(portals);
}

static int on_request(struct xio_session *session,
            struct xio_msg *req,
            int last_in_rxq,
            void *cb_user_context)
{
    struct xio_msg *rsp;
    struct io_worker_data *wdata;
    struct xio_iovec_ex *in_sglist;
    struct xio_iovec_ex *out_sglist;
    struct rdb_req_hdr *req_hdr;
    void *ptr;
    int *status;
    char *value;
    size_t key_size, value_size;
//    int len;

    wdata = cb_user_context;
    rsp = &wdata->rsp;

    in_sglist = vmsg_sglist(&req->in);
    req_hdr = in_sglist[0].iov_base;
    ptr = in_sglist[0].iov_base;
    ptr += sizeof (*req_hdr);

    switch (req_hdr->rdb_command) {
        case RDB_CMD_PUT:
            {
                struct rdb_put_req_hdr *put_hdr;

                put_hdr = ptr;

                struct __attribute__((__packed__)) rdb_put_req {
                    struct rdb_key {
                        struct  rdb_key_hdr     key_hdr;
                        char                    key_data[put_hdr->key_size];
                    } key;
                    struct rdb_value {
                        struct  rdb_value_hdr   value_hdr;
                        char                    value_data[put_hdr->value_size];
                    } value;
                } *put_req;

                ptr += sizeof (*put_hdr);
                put_req = ptr;
                key_size = sizeof (struct rdb_key);
                value_size = sizeof (struct rdb_value);
                if (!null_mode)
                    if (!rocksdb_server_put(wdata->rdb, (char *)&put_req->key, &key_size,
                            (char *)&put_req->value, &value_size)) {
                        fprintf(stderr, "rocksdb put failed\n");
                    }
//                len = (int) in_sglist[0].iov_len;

                break;
            }
        case RDB_CMD_MPUT:
            {
                struct rdb_mput_req_hdr *mput_hdr;
                char *records;

                mput_hdr = ptr;

                struct __attribute__((__packed__)) rdb_mput_req {
                    struct multi_kv_pairs {
                        struct rdb_key {
                            struct  rdb_key_hdr     key_hdr;
                            char                    key_data[mput_hdr->key_size];
                        } key;
                        struct rdb_value {
                            struct  rdb_value_hdr   value_hdr;
                            char                    value_data[mput_hdr->value_size];
                        } value;
                    } m_kv_pairs[mput_hdr->num_records];
                } *mput_req;

                ptr += sizeof (*mput_hdr);
                mput_req = ptr;
                key_size = sizeof (struct rdb_key);
                value_size = sizeof (struct rdb_value);
                records = (char *)mput_req->m_kv_pairs;

                if (!null_mode)
                    if (!rocksdb_server_mput(wdata->rdb, records, &key_size,
                            &value_size, &mput_hdr->num_records)) {
                        fprintf(stderr, "rocksdb mput failed\n");
                    }
//                len = (int) in_sglist[0].iov_len;

                break;
            }
        case RDB_CMD_GET:
            {
                struct rdb_get_req_hdr *get_hdr;

                get_hdr = ptr;

                struct __attribute__((__packed__)) rdb_get_req {
                    struct rdb_key {
                        struct  rdb_key_hdr     key_hdr;
                        char                    key_data[get_hdr->key_size];
                    } key;
                } *get_req;

                ptr += sizeof (*get_hdr);
                get_req = ptr;
                key_size = sizeof (get_req->key);

                out_sglist = vmsg_sglist(&rsp->out);
                out_sglist[0].iov_base = wdata->reg_mem.addr;
                out_sglist[0].mr = wdata->reg_mem.mr;

                if (!null_mode) {
                    status = wdata->reg_mem.addr;
                    value = wdata->reg_mem.addr + sizeof (*status);
                    *status = rocksdb_server_get(wdata->rdb, (char *)&get_req->key, &key_size,
                                    value, &value_size);
                    if (*status)
                        out_sglist[0].iov_len = value_size + sizeof (*status);
                    else
                        out_sglist[0].iov_len = sizeof (*status);
                } else {
                    status = wdata->reg_mem.addr;
                    *status = 1;
                    out_sglist[0].iov_len = sizeof (*status);
                }
//                len = (int) out_sglist[0].iov_len;

                vmsg_sglist_set_nents(&rsp->out, 1);

                break;
            }
        case RDB_CMD_MGET:
            {
                struct rdb_mget_req_hdr *mget_hdr;
                char *records;

                mget_hdr = ptr;

                struct __attribute__((__packed__)) rdb_mget_req {
                    struct multi_k_pairs {
                        struct rdb_key {
                            struct  rdb_key_hdr     key_hdr;
                            char                    key_data[mget_hdr->key_size];
                        } key;
                    } m_k_pairs[mget_hdr->num_records];
                } *mget_req;

                ptr += sizeof (*mget_hdr);
                mget_req = ptr;
                key_size = sizeof (struct rdb_key);
                records = (char *) mget_req->m_k_pairs;

                out_sglist = vmsg_sglist(&rsp->out);
                out_sglist[0].iov_base = wdata->reg_mem.addr;
                out_sglist[0].mr = wdata->reg_mem.mr;

                if (!null_mode) {
                    status = wdata->reg_mem.addr;
                    value = wdata->reg_mem.addr + sizeof (*status);
                    *status = rocksdb_server_mget(wdata->rdb, records, &key_size,
                                    value, &value_size, &mget_hdr->num_records);
                    if (*status)
                        out_sglist[0].iov_len = value_size + sizeof (*status);
                    else
                        out_sglist[0].iov_len = sizeof (*status);
                } else {
                    status = wdata->reg_mem.addr;
                    *status = 1;
                    out_sglist[0].iov_len = sizeof (*status);
                }
//                len = (int) out_sglist[0].iov_len;

                vmsg_sglist_set_nents(&rsp->out, 1);

                break;
            }
        default:
            break;
    };

    /*printf("thread : portal : %s, command : %d, version : %d, key : %s, value : %s, len : %d\n",
            wdata->portal,
            *payload_cmd,
            *payload_version,
            key,
            value,
            len);*/

    in_sglist[0].iov_base = NULL;
    in_sglist[0].iov_len = 0;
    vmsg_sglist_set_nents(&req->in, 0);

    rsp->request = req;
    if (xio_send_response(rsp) == -1) {
        fprintf(stderr, "failed to send response for thread %s. reason %d - (%s)\n",
                wdata->portal, xio_errno(), xio_strerror(xio_errno()));
    }

    return (0);
}

static struct xio_session_ops io_worker_ops = {
    .on_session_event       =  NULL,
    .on_new_session         =  NULL,
    .on_msg_send_complete   =  NULL,
    .on_msg                 =  on_request,
    .on_msg_error           =  NULL,
    .assign_data_in_buf     =  NULL
};

/* Server I/O manager */
void *rdb_r_s_io_worker(void *data)
{
    struct io_worker_data *worker_data;
    struct xio_server *server = NULL;
    cpu_set_t cpuset;

    worker_data = (struct io_worker_data *) data;

    /* set affinity to thread */
    CPU_ZERO(&cpuset);
    CPU_SET(worker_data->affinity, &cpuset);
    pthread_setaffinity_np(worker_data->thread_id, sizeof(cpu_set_t), &cpuset);

    worker_data->ctx = xio_context_create(NULL, POLLING_TIMEOUT,
                                            worker_data->affinity);
    if (!worker_data->ctx) {
        fprintf(stderr, "failed to create context for thread %s. reason %d - (%s)\n",
                worker_data->portal, xio_errno(), xio_strerror(xio_errno()));
        goto out;
    }
    server = xio_bind(worker_data->ctx, &io_worker_ops,
                worker_data->portal, NULL, 0, worker_data);
    if (!server) {
        fprintf(stderr, "failed to bind context for thread %s. reason %d - (%s)\n",
                worker_data->portal, xio_errno(), xio_strerror(xio_errno()));
        goto out;
    }

    xio_mem_alloc(MAX_VALUE_SIZE, &worker_data->reg_mem);
    if (!worker_data->reg_mem.addr) {
        fprintf(stderr, "failed to allocate accelio transfer buffer for thread %s. reason %d - (%s)\n",
                worker_data->portal, xio_errno(), xio_strerror(xio_errno()));
        goto out;
    }

    /* the default xio supplied main loop */
    if (xio_context_run_loop(worker_data->ctx, XIO_INFINITE) != 0) {
        fprintf(stderr, "failed to run event loop for thread %s. reason %d - (%s)\n",
                worker_data->portal, xio_errno(), xio_strerror(xio_errno()));
    }

out:
    /* detach the worker */
    if (server)
        xio_unbind(server);

    if (worker_data->reg_mem.addr)
        xio_mem_free(&worker_data->reg_mem);

    /* free the context */
    if (worker_data->ctx)
        xio_context_destroy(worker_data->ctx);

    return NULL;
}

/* Create io_worker threads */
void rdb_r_s_create_workers(struct server_data *server_data, void *rdb, int num_threads)
{
    uint64_t cpusmask;
    int max_cpus;
    int cpusnr;
    int cpu;
    int i;
    int port;

    num_threads = (num_threads <= MAX_WORKERS) ? num_threads : MAX_WORKERS;

    max_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    (void) intf_best_cpus(server_data->host, &cpusmask, &cpusnr);

    port = server_data->port;
    for (i = 0, cpu = 0; i < num_threads; i++, cpu++) {
        /*while (1) {
            if (cpusmask_test_bit(cpu, &cpusmask))
                break;
            if (++cpu == max_cpus)
                cpu = 0;
        }*/
        if (++cpu == max_cpus)
            cpu = 0;
        port++;
        server_data->wdata[i].affinity = cpu;
        sprintf(server_data->wdata[i].portal, "%s://%s:%d", TRANSPORT, server_data->host, port);
        server_data->wdata[i].rdb = rdb;
        pthread_create(&server_data->wdata[i].thread_id, NULL,
                rdb_r_s_io_worker, &server_data->wdata[i]);
        server_data->nworkers++;
        printf("server thread created with portal %s\n",
                server_data->wdata[i].portal);
    }
}

static int on_session_event(struct xio_session *session,
        struct xio_session_event_data *event_data,
        void *cb_user_context)
{
    struct server_data *server_data;
    int i;

    server_data = (struct server_data *) cb_user_context;

    printf("session event: %s. session:%p, connection:%p, reason: %s\n",
            xio_session_event_str(event_data->event),
            session, event_data->conn,
            xio_strerror(event_data->reason));

    switch (event_data->event) {
        case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
            xio_connection_destroy(event_data->conn);
            break;
        case XIO_SESSION_TEARDOWN_EVENT:
            xio_session_destroy(session);
            for (i = 0; i < server_data->nworkers; i++) {
                xio_context_stop_loop(server_data->wdata[i].ctx);
            }
            xio_context_stop_loop(server_data->ctx);
        default:
            break;
    };

    return (0);
}

static int on_new_session(struct xio_session *session,
        struct xio_new_session_req *req,
        void *cb_user_context)
{
    struct portals_vec *portals;
    struct server_data *server_data;

    server_data = (struct server_data *) cb_user_context;

    portals = portals_get(server_data, req->uri, req->private_data);

    /* automatic accept the request */
    xio_accept(session, portals->vec, portals->vec_len, NULL, 0);

    portals_free(portals);

    return (0);
}

static struct xio_session_ops   server_ops = {
    .on_session_event       =  on_session_event,
    .on_new_session         =  on_new_session,
    .on_msg_send_complete   =  NULL,
    .on_msg                 =  NULL,
    .on_msg_error           =  NULL
};

/* Server session manager */
struct server_data* rdb_r_s_server_create(const char *host, const int *port)
{
    struct server_data *server_data = NULL;
    char url[NAME_LEN];

    server_data = (struct server_data *)calloc(1, sizeof(*server_data));
    if (!server_data) {
        fprintf(stderr, "failed to allocate memory for server_data\n");
        return (NULL);
    }

    /* init the accelio library */
    xio_init();

    /* create thread context */
    server_data->ctx = xio_context_create(NULL, 0, -1);
    if (!server_data->ctx) {
        fprintf(stderr, "failed to create context for server listner. reason %d - (%s)\n",
                xio_errno(), xio_strerror(xio_errno()));
        goto out;
    }

    sprintf(url, "%s://%s:%d", TRANSPORT, host, *port);
    strcpy(server_data->host, host);
    server_data->port = *port;

    /* bind a listner */
    server_data->server = xio_bind(server_data->ctx, &server_ops,
                                    url, NULL, 0, server_data);
    if (!server_data->server) {
        fprintf(stderr, "failed to bind server listner. reason %d - (%s)\n",
                xio_errno(), xio_strerror(xio_errno()));
        goto out;
    }

out:
    return (server_data);
}

/* Server run loop till shutdown */
void rdb_r_s_server_run(struct server_data *server_data)
{
    int i;

    /* default xio supplied main loop */
    if (xio_context_run_loop(server_data->ctx, XIO_INFINITE) != 0) {
        for (i = 0; i < server_data->nworkers; i++) {
            xio_context_stop_loop(server_data->wdata[i].ctx);
        }
        fprintf(stderr, "failed to run event loop for server listner. reason %d - (%s)\n",
                xio_errno(), xio_strerror(xio_errno()));
    }

    /* join the worker threads */
    for (i = 0; i < server_data->nworkers; i++) {
        pthread_join(server_data->wdata[i].thread_id, NULL);
    }

    /* free the server */
    if (server_data->server)
        xio_unbind(server_data->server);

    /* free the context */
    if (server_data->ctx)
        xio_context_destroy(server_data->ctx);

    xio_shutdown();

    if (server_data)
        free(server_data);
}


static void usage()
{
    printf("Usage:\n");
    printf("\t-i    host address of server to connect\n");
    printf("\t-p    port\n");
    printf("\t-t    number of threads\n");
    printf("\t-n    null mode (optional)\n");
    printf("\t-h    help\n");
}

int main(int argc, char *argv[])
{
    void /* *data, */ *rdb;
    struct server_data *server_data = NULL;
    char host[NAME_LEN];
    int port;
    int n_threads;
    int c;
    int min_options = 0;

    n_threads = 1;

    opterr = 0;
    while ((c = getopt(argc, argv, "i:p:t:nh")) != -1) {
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
            case 'n':
                null_mode = 1;
                break;
            case 'h':
                usage();
                exit(0);
            default:
                fprintf(stderr, "invalid command option\n");
                usage();
                exit(0);
        };
    }
    if (min_options < 2) {
        fprintf(stderr, "host address and port are required\n");
        usage();
        exit(1);
    }

    rdb = new_rocksdb_server();

    server_data = rdb_r_s_server_create(host, &port);
    if (!server_data)
        exit(1);

    rdb_r_s_create_workers(server_data, rdb, n_threads);

    rdb_r_s_server_run(server_data);

    delete_rocksdb_server(rdb);

    return (0);
}
