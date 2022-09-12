#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cli.h"
#include "../include/raft.h"
#include "../include/raft/uv.h"

#define N_SERVERS 3    /* Number of servers in the example cluster */
#define APPLY_RATE 125 /* Apply a new entry every 125 milliseconds */

#define Log(SERVER_ID, FORMAT) printf("%lld: " FORMAT "\n", SERVER_ID)
#define Logf(SERVER_ID, FORMAT, ...) \
    printf("%lld: " FORMAT "\n", SERVER_ID, __VA_ARGS__) \

/********************************************************************
 *
 * Sample application FSM that just increases a counter.
 *
 ********************************************************************/

struct Fsm
{
    unsigned long long count;
};

static int FsmApply(struct raft_fsm *fsm,
                    const struct raft_buffer *buf,
                    void **result)
{
    struct Fsm *f = fsm->data;
    if (buf->len != 8) {
        return RAFT_MALFORMED;
    }
    f->count += *(uint64_t *)buf->base;
    *result = &f->count;
    return 0;
}

static int FsmSnapshot(struct raft_fsm *fsm,
                       struct raft_buffer *bufs[],
                       unsigned *n_bufs)
{
    struct Fsm *f = fsm->data;
    *n_bufs = 1;
    *bufs = raft_malloc(sizeof **bufs);
    if (*bufs == NULL) {
        return RAFT_NOMEM;
    }
    (*bufs)[0].len = sizeof(uint64_t);
    (*bufs)[0].base = raft_malloc((*bufs)[0].len);
    if ((*bufs)[0].base == NULL) {
        return RAFT_NOMEM;
    }
    *(uint64_t *)(*bufs)[0].base = f->count;
    return 0;
}

static int FsmRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    struct Fsm *f = fsm->data;
    if (buf->len != sizeof(uint64_t)) {
        return RAFT_MALFORMED;
    }
    f->count = *(uint64_t *)buf->base;
    raft_free(buf->base);
    return 0;
}

static int FsmInit(struct raft_fsm *fsm)
{
    struct Fsm *f = raft_malloc(sizeof *f);
    if (f == NULL) {
        return RAFT_NOMEM;
    }
    f->count = 0;
    fsm->version = 2;
    fsm->data = f;
    fsm->apply = FsmApply;
    fsm->snapshot = FsmSnapshot;
    fsm->snapshot_finalize = NULL;
    fsm->restore = FsmRestore;
    return 0;
}

static void FsmClose(struct raft_fsm *f)
{
    if (f->data != NULL) {
        raft_free(f->data);
    }
}

/********************************************************************
 *
 * Example struct holding a single raft server instance and all its
 * dependencies.
 *
 ********************************************************************/

struct Server;
typedef void (*ServerCloseCb)(struct Server *server);

struct Server
{
    void *data;                         /* User data context. */
    struct uv_loop_s *loop;             /* UV loop. */
    const char *dir;                    /* Data dir of UV I/O backend. */
    struct raft_uv_transport transport; /* UV I/O backend transport. */
    struct raft_io io;                  /* UV I/O backend. */
    struct raft_fsm fsm;                /* Sample application FSM. */
    unsigned long long id;              /* Raft instance ID. */
    char address[64];                   /* Raft instance address. */
    struct raft raft;                   /* Raft instance. */
    struct raft_transfer transfer;      /* Transfer leadership request. */
    ServerCloseCb close_cb;             /* Optional close callback. */
};

struct Server *server;

static void serverRaftCloseCb(struct raft *raft)
{
    struct Server *s = raft->data;
    raft_uv_close(&s->io);
    raft_uv_tcp_close(&s->transport);
    FsmClose(&s->fsm);
    if (s->close_cb != NULL) {
        s->close_cb(s);
    }
}

static void serverTransferCb(struct raft_transfer *req)
{
    struct Server *s = req->data;
    raft_id id;
    const char *address;
    raft_leader(&s->raft, &id, &address);
    raft_close(&s->raft, serverRaftCloseCb);
}

/* Final callback in the shutdown sequence, invoked after the timer handle has
 * been closed. */
static void serverTimerCloseCb(struct uv_handle_s *handle)
{
    struct Server *s = handle->data;
    if (s->raft.data != NULL) {
        if (s->raft.state == RAFT_LEADER) {
            int rv;
            rv = raft_transfer(&s->raft, &s->transfer, 0, serverTransferCb);
            if (rv == 0) {
                return;
            }
        }
        raft_close(&s->raft, serverRaftCloseCb);
    }
}

/* Initialize the example server struct, without starting it yet. */
static int ServerInit(struct Server *s,
                      struct uv_loop_s *loop,
                      const char *dir,
                      unsigned id,
                      const char *address)
{
    struct raft_configuration configuration;
    struct timespec now;
    int rv;

    memset(s, 0, sizeof *s);

    /* Seed the random generator */
    timespec_get(&now, TIME_UTC);
    srandom((unsigned)(now.tv_nsec ^ now.tv_sec));

    s->loop = loop;

    /* Initialize the TCP-based RPC transport. */
    rv = raft_uv_tcp_init(&s->transport, s->loop);
    if (rv != 0) {
        goto err;
    }

    /* Initialize the libuv-based I/O backend. */
    rv = raft_uv_init(&s->io, s->loop, dir, &s->transport);
    if (rv != 0) {
        Logf(s->id, "raft_uv_init(): %s", s->io.errmsg);
        goto err_after_uv_tcp_init;
    }

    /* Initialize the finite state machine. */
    rv = FsmInit(&s->fsm);
    if (rv != 0) {
        Logf(s->id, "FsmInit(): %s", raft_strerror(rv));
        goto err_after_uv_init;
    }

    /* Save the server ID and address. */
    s->id = id;
    strcpy(s->address, address);

    /* Initialize and start the engine, using the libuv-based I/O backend. */
    rv = raft_init(&s->raft, &s->io, &s->fsm, id, s->address);
    if (rv != 0) {
        Logf(s->id, "raft_init(): %s", raft_errmsg(&s->raft));
        goto err_after_fsm_init;
    }
    s->raft.data = s;

    /* Bootstrap the initial configuration if needed. */
    raft_configuration_init(&configuration);
    rv = raft_configuration_add(&configuration, id, address, RAFT_VOTER);
    if (rv != 0) {
        Logf(s->id, "raft_configuration_add(): %s", raft_strerror(rv));
        goto err_after_configuration_init;
    }
    rv = raft_bootstrap(&s->raft, &configuration);
    if (rv != 0 && rv != RAFT_CANTBOOTSTRAP) {
        goto err_after_configuration_init;
    }
    raft_configuration_close(&configuration);

    raft_set_snapshot_threshold(&s->raft, 64);
    raft_set_snapshot_trailing(&s->raft, 16);
    raft_set_pre_vote(&s->raft, true);

    s->transfer.data = s;

    return 0;

err_after_configuration_init:
    raft_configuration_close(&configuration);
err_after_fsm_init:
    FsmClose(&s->fsm);
err_after_uv_init:
    raft_uv_close(&s->io);
err_after_uv_tcp_init:
    raft_uv_tcp_close(&s->transport);
err:
    return rv;
}

/* Called after a request to apply a new command to the FSM has been
 * completed. */
static void serverApplyCb(struct raft_apply *req, int status, void *result)
{
    struct Server *s = req->data;
    int count;
    raft_free(req);
    if (status != 0) {
        if (status != RAFT_LEADERSHIPLOST) {
            cli_printf("%lld: raft_apply() callback: %s (%d)\n", s->id, raft_errmsg(&s->raft), status);
        }
        cli_cmd_end();
        return;
    }
    count = *(int *)result;
    if (count % 100 == 0) {
        cli_printf("%lld: count %d\n", s->id, count);
    }
    cli_cmd_end();
}

/* Called when received client's apply command. */
static void serverApply(int argc, char *argv[])
{
    struct raft_buffer buf;
    struct raft_apply *req;
    int rv;

    if (server->raft.state != RAFT_LEADER) {
        return;
    }

    buf.len = sizeof(uint64_t);
    buf.base = raft_malloc(buf.len);
    if (buf.base == NULL) {
        cli_printf("%lld: serverApply(): out of memory\n", server->id);
        cli_cmd_end();
        return;
    }

    *(uint64_t *)buf.base = 1;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        cli_printf("%lld: serverApply(): out of memory\n", server->id);
        cli_cmd_end();
        return;
    }
    req->data = server;

    rv = raft_apply(&server->raft, req, &buf, 1, serverApplyCb);
    if (rv != 0) {
        cli_printf("%lld: raft_apply(): %s\n", server->id, raft_errmsg(&server->raft));
        cli_cmd_end();
        return;
    }
}

/* Called after a request to add a new server to the cluster has been
 * completed. */
void serverAddCb(struct raft_change *req, int status) {
    struct Server *s = req->data;
    raft_free(req);
    if (status != 0) {
        if (status != RAFT_LEADERSHIPLOST) {
            cli_printf("%lld: raft_add() callback: %s (%d)\n", s->id, raft_errmsg(&s->raft), status);
        }
        cli_cmd_end();
        return;
    }
    cli_printf("%lld: add server succeeded\n", s->id);
    cli_cmd_end();
}

/* Called when received client's add server command. */
static void serverAdd(int argc, char *argv[])
{
    raft_id id = strtoll(argv[1], NULL, 10);
    char *address = argv[2];
    struct raft_change *req;
    int rv;

    if (server->raft.state != RAFT_LEADER) {
        return;
    }

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        cli_printf("%lld: serverAdd(): out of memory\n", server->id);
        cli_cmd_end();
        return;
    }
    req->data = server;

    rv = raft_add(&server->raft, req, id, address, serverAddCb);
    if (rv != 0) {
        cli_printf("%lld: raft_add(): %s\n", server->id, raft_errmsg(&server->raft));
        cli_cmd_end();
        return;
    }
}

/* Called after a request to remove a server from the cluster has been
 * completed. */
void serverRemoveCb(struct raft_change *req, int status) {
    struct Server *s = req->data;
    raft_free(req);
    if (status != 0) {
        if (status != RAFT_LEADERSHIPLOST) {
            cli_printf("%lld: raft_remove() callback: %s (%d)\n", s->id, raft_errmsg(&s->raft), status);
        }
        cli_cmd_end();
        return;
    }
    cli_printf("%lld: remove server succeeded\n", s->id);
    cli_cmd_end();
}

/* Called when received client's remove server command. */
static void serverRemove(int argc, char *argv[])
{
    raft_id id = strtoll(argv[1], NULL, 10);
    struct raft_change *req;
    int rv;

    if (server->raft.state != RAFT_LEADER) {
        return;
    }

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        cli_printf("%lld: serverRemove(): out of memory\n", server->id);
        cli_cmd_end();
        return;
    }
    req->data = server;

    rv = raft_remove(&server->raft, req, id, serverRemoveCb);
    if (rv != 0) {
        cli_printf("%lld: raft_remove(): %s\n", server->id, raft_errmsg(&server->raft));
        cli_cmd_end();
        return;
    }
}

/* Start the example server. */
static int ServerStart(struct Server *s)
{
    int rv;

    Log(s->id, "starting");

    rv = raft_start(&s->raft);
    if (rv != 0) {
        Logf(s->id, "raft_start(): %s", raft_errmsg(&s->raft));
        goto err;
    }

    return 0;

err:
    return rv;
}

/* Release all resources used by the example server. */
static void ServerClose(struct Server *s, ServerCloseCb cb)
{
    s->close_cb = cb;
    Log(s->id, "stopping");
    s->close_cb(s);
}

/********************************************************************
 *
 * Top-level main loop.
 *
 ********************************************************************/

static void mainServerCloseCb(struct Server *s)
{
    struct uv_signal_s *sigint = s->data;
    uv_close((struct uv_handle_s *)sigint, NULL);
}

/* Handler triggered by SIGINT. It will initiate the shutdown sequence. */
static void mainSigintCb(struct uv_signal_s *handle, int signum)
{
    struct Server *s = handle->data;
    assert(signum == SIGINT);
    uv_signal_stop(handle);
    s->data = handle;
    ServerClose(s, mainServerCloseCb);
}

int main(int argc, char *argv[])
{
    struct uv_loop_s loop;
    struct uv_signal_s sigint; /* To catch SIGINT and exit. */
    const char *dir;
    const char *address;
    unsigned long long id;
    char *socketPath;
    char *endPtr;
    int rv;

    if (argc != 5) {
        printf("usage: example-server <dir> <id> <address> <socketPath>\n");
        return 1;
    }
    dir = argv[1];
    address = argv[3];
    socketPath = argv[4];
    errno = 0;
    id = (unsigned)strtoll(argv[2], &endPtr, 10);
    if (errno != 0 || id == 0 || *endPtr != '\0') {
        printf("Invalid id, exiting.\n");
        return -1;
    }

    /* Ignore SIGPIPE, see https://github.com/joyent/libuv/issues/1254 */
    signal(SIGPIPE, SIG_IGN);

    /* Register commands. */
    init_cli_server(socketPath);
    cli_reg_cmd("apply", "apply a client command to cluster.", serverApply);
    cli_reg_cmd("add", "add a new server to cluster.", serverAdd);
    cli_reg_cmd("remove", "remove a server from cluster.", serverRemove);

    /* Initialize the libuv loop. */
    rv = uv_loop_init(&loop);
    if (rv != 0) {
        Logf(id, "uv_loop_init(): %s", uv_strerror(rv));
        goto err;
    }

    server = malloc(sizeof(struct Server));

    /* Initialize the example server. */
    rv = ServerInit(server, &loop, dir, id, address);
    if (rv != 0) {
        goto err_after_server_init;
    }

    /* Add a signal handler to stop the example server upon SIGINT. */
    rv = uv_signal_init(&loop, &sigint);
    if (rv != 0) {
        Logf(id, "uv_signal_init(): %s", uv_strerror(rv));
        goto err_after_server_init;
    }
    sigint.data = &server;
    rv = uv_signal_start(&sigint, mainSigintCb, SIGINT);
    if (rv != 0) {
        Logf(id, "uv_signal_start(): %s", uv_strerror(rv));
        goto err_after_signal_init;
    }

    /* Start the server. */
    rv = ServerStart(server);
    if (rv != 0) {
        goto err_after_signal_init;
    }

    /* Run the event loop until we receive SIGINT. */
    rv = uv_run(&loop, UV_RUN_DEFAULT);
    if (rv != 0) {
        Logf(id, "uv_run_start(): %s", uv_strerror(rv));
    }

    uv_loop_close(&loop);

    // exit_cli_server();

    return rv;

err_after_signal_init:
    uv_close((struct uv_handle_s *)&sigint, NULL);
err_after_server_init:
    ServerClose(server, NULL);
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
err:
    return rv;
}
