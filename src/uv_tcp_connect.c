#include <string.h>

#include "assert.h"
#include "byte.h"
#include "err.h"
#include "heap.h"
#include "uv_ip.h"
#include "uv_tcp.h"

/* The happy path of a connection request is:
 *
 * - Create a TCP handle and submit a TCP connect request.
 * - Once connected over TCP, submit a write request for the handshake.
 * - Once the write completes, fire the connection request callback.
 *
 * Possible failure modes are:
 *
 * - The transport get closed, close the TCP handle and and fire the request
 *   callback with RAFT_CANCELED.
 *
 * - Either the TCP connect or the write request fails: close the TCP handle and
 *   fire the request callback with RAFT_NOCONNECTION.
 */

/* Hold state for a single connection request. */
struct uvTcpConnect
{
    struct UvTcp *t;                     /* Transport implementation */
    struct raft_uv_connect *req;         /* User request */
    uv_buf_t handshake;                  /* Handshake data */
    struct uv_tcp_s *tcp;                /* TCP connection socket handle */
    struct uv_getaddrinfo_s getaddrinfo; /* DNS resolve request */
    struct uv_connect_s connect;         /* TCP connection request */
    struct uv_write_s write;             /* TCP handshake request */
    int status;                          /* Returned to the request callback */
    bool resolving; /* Indicate name resolving in progress */
    queue queue;    /* Pending connect queue */
};

/* Encode an handshake message into the given buffer. */
static int uvTcpEncodeHandshake(raft_id id, const char *address, uv_buf_t *buf)
{
    void *cursor;
    size_t address_len = bytePad64(strlen(address) + 1);
    buf->len = sizeof(uint64_t) + /* Protocol version. */
               sizeof(uint64_t) + /* Server ID. */
               sizeof(uint64_t) /* Size of the address buffer */;
    buf->len += address_len;
    buf->base = RaftHeapMalloc(buf->len);
    if (buf->base == NULL) {
        return RAFT_NOMEM;
    }
    cursor = buf->base;
    bytePut64(&cursor, UV__TCP_HANDSHAKE_PROTOCOL);
    bytePut64(&cursor, id);
    bytePut64(&cursor, address_len);
    strcpy(cursor, address);
    return 0;
}

/* Finish the connect request, releasing its memory and firing the connect
 * callback. */
static void uvTcpConnectFinish(struct uvTcpConnect *connect)
{
    struct uv_stream_s *stream = (struct uv_stream_s *)connect->tcp;
    struct raft_uv_connect *req = connect->req;
    int status = connect->status;
    QUEUE_REMOVE(&connect->queue);
    RaftHeapFree(connect->handshake.base);
    uv_freeaddrinfo(connect->getaddrinfo.addrinfo);
    raft_free(connect);
    req->cb(req, stream, status);
}

/* The TCP connection handle has been closed in consequence of an error or
 * because the transport is closing. */
static void uvTcpConnectUvCloseCb(struct uv_handle_s *handle)
{
    struct uvTcpConnect *connect = handle->data;
    struct UvTcp *t = connect->t;
    assert(connect->status != 0);
    assert(handle == (struct uv_handle_s *)connect->tcp);
    RaftHeapFree(connect->tcp);
    connect->tcp = NULL;
    uvTcpConnectFinish(connect);
    UvTcpMaybeFireCloseCb(t);
}

/* Abort a connection request. */
static void uvTcpConnectAbort(struct uvTcpConnect *connect)
{
    QUEUE_REMOVE(&connect->queue);
    QUEUE_PUSH(&connect->t->aborting, &connect->queue);
    uv_cancel((struct uv_req_s *)&connect->getaddrinfo);
    /* Call uv_close on the tcp handle, if there is no getaddrinfo request
in flight. Data structures may only be freed after the uvGetAddrInfoCb was
triggered. Tcp handle will be closed in the uvGetAddrInfoCb in this case. */
    if (!connect->resolving) {
        uv_close((struct uv_handle_s *)connect->tcp, uvTcpConnectUvCloseCb);
    }
}

/* The handshake TCP write completes. Fire the connect callback. */
static void uvTcpConnectUvWriteCb(struct uv_write_s *write, int status)
{
    struct uvTcpConnect *connect = write->data;
    struct UvTcp *t = connect->t;

    if (t->closing) {
        connect->status = RAFT_CANCELED;
        return;
    }

    if (status != 0) {
        assert(status != UV_ECANCELED); /* t->closing would have been true */
        connect->status = RAFT_NOCONNECTION;
        uvTcpConnectAbort(connect);
        return;
    }

    uvTcpConnectFinish(connect);
}

/* The TCP connection is established. Write the handshake data. */
static void uvTcpConnectUvConnectCb(struct uv_connect_s *req, int status)
{
    struct uvTcpConnect *connect = req->data;
    struct UvTcp *t = connect->t;
    int rv;

    if (t->closing) {
        connect->status = RAFT_CANCELED;
        return;
    }

    if (status != 0) {
        assert(status != UV_ECANCELED); /* t->closing would have been true */
        connect->status = RAFT_NOCONNECTION;
        ErrMsgPrintf(t->transport->errmsg, "uv_tcp_connect(): %s",
                     uv_strerror(status));
        goto err;
    }

    rv = uv_write(&connect->write, (struct uv_stream_s *)connect->tcp,
                  &connect->handshake, 1, uvTcpConnectUvWriteCb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        connect->status = RAFT_NOCONNECTION;
        goto err;
    }

    return;

err:
    uvTcpConnectAbort(connect);
}

/* The hostname resolve is finished */
static void uvGetAddrInfoCb(uv_getaddrinfo_t *req,
                            int status,
                            struct addrinfo *res)
{
    struct uvTcpConnect *connect = req->data;
    struct UvTcp *t = connect->t;
    int rv;

    connect->resolving =
        false; /* Indicate we are in the name resolving phase */

    if (t->closing) {
        connect->status = RAFT_CANCELED;

        /* We need to close the tcp handle to to connection attempt */
        uv_close((struct uv_handle_s *)connect->tcp, uvTcpConnectUvCloseCb);
        return;
    }

    if (status < 0) {
        ErrMsgPrintf(t->transport->errmsg, "uv_getaddrinfo(): %s",
                     uv_err_name(status));
        connect->status = RAFT_NOCONNECTION;
        goto err;
    }
    rv = uv_tcp_connect(&connect->connect, connect->tcp,
                        (const struct sockaddr *)res->ai_addr,
                        uvTcpConnectUvConnectCb);
    if (rv != 0) {
        /* UNTESTED: since parsing succeed, this should fail only because of
         * lack of system resources */
        ErrMsgPrintf(t->transport->errmsg, "uv_tcp_connect(): %s",
                     uv_strerror(rv));
        connect->status = RAFT_NOCONNECTION;
        goto err;
    }

    return;

err:
    uvTcpConnectAbort(connect);
}
/* Create a new TCP handle and submit a connection request to the event loop. */
static int uvTcpConnectStart(struct uvTcpConnect *r, const char *address)
{
    static struct addrinfo hints = {
        .ai_flags = AI_V4MAPPED | AI_ADDRCONFIG | AI_NUMERICHOST,
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = 0};
    struct UvTcp *t = r->t;
    char hostname[NI_MAXHOST];
    char service[NI_MAXSERV];
    int rv;

    r->handshake.base = NULL;

    /* Initialize the handshake buffer. */
    rv = uvTcpEncodeHandshake(t->id, t->address, &r->handshake);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM);
        ErrMsgOom(t->transport->errmsg);
        goto err;
    }

    r->tcp = RaftHeapMalloc(sizeof *r->tcp);
    if (r->tcp == NULL) {
        ErrMsgOom(t->transport->errmsg);
        rv = RAFT_NOMEM;
        goto err;
    }

    rv = uv_tcp_init(r->t->loop, r->tcp);
    assert(rv == 0);
    r->tcp->data = r;

    rv = uvIpAddrSplit(address, hostname, sizeof(hostname), service,
                       sizeof(service));
    if (rv) {
        ErrMsgPrintf(t->transport->errmsg,
                     "uv_tcp_connect(): Cannot split %s into host and service",
                     address);
        rv = RAFT_NOCONNECTION;
        goto err_after_tcp_init;
    }
    rv = uv_getaddrinfo(r->t->loop, &r->getaddrinfo, &uvGetAddrInfoCb, hostname,
                        service, &hints);
    if (rv) {
        ErrMsgPrintf(t->transport->errmsg,
                     "uv_tcp_connect(): Cannot initiate getaddrinfo %s",
                     uv_strerror(rv));
        rv = RAFT_NOCONNECTION;
        goto err_after_tcp_init;
    }
    r->resolving = true; /* Indicate we are in the name resolving phase */

    return 0;

err_after_tcp_init:
    uv_close((uv_handle_t *)r->tcp, (uv_close_cb)RaftHeapFree);

err:
    RaftHeapFree(r->handshake.base);

    return rv;
}

int UvTcpConnect(struct raft_uv_transport *transport,
                 struct raft_uv_connect *req,
                 raft_id id,
                 const char *address,
                 raft_uv_connect_cb cb)
{
    struct UvTcp *t = transport->impl;
    struct uvTcpConnect *r;
    int rv;
    (void)id;
    assert(!t->closing);

    /* Create and initialize a new TCP connection request object */
    r = RaftHeapMalloc(sizeof *r);
    if (r == NULL) {
        rv = RAFT_NOMEM;
        ErrMsgOom(transport->errmsg);
        goto err;
    }
    r->t = t;
    r->req = req;
    r->status = 0;
    r->write.data = r;
    r->getaddrinfo.data = r;
    r->resolving = false;
    r->connect.data = r;
    req->cb = cb;

    /* Keep track of the pending request */
    QUEUE_PUSH(&t->connecting, &r->queue);

    /* Start connecting */
    rv = uvTcpConnectStart(r, address);
    if (rv != 0) {
        goto err_after_alloc;
    }

    return 0;

err_after_alloc:
    QUEUE_REMOVE(&r->queue);
    RaftHeapFree(r);
err:
    return rv;
}

void UvTcpConnectClose(struct UvTcp *t)
{
    while (!QUEUE_IS_EMPTY(&t->connecting)) {
        struct uvTcpConnect *connect;
        queue *head;
        head = QUEUE_HEAD(&t->connecting);
        connect = QUEUE_DATA(head, struct uvTcpConnect, queue);
        uvTcpConnectAbort(connect);
    }
}
