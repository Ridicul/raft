//
// Created by yjzzjy4 on 8/21/22.
//

#ifndef RAFT_ANOTHER_SERVER_H
#define RAFT_ANOTHER_SERVER_H

#endif  // RAFT_ANOTHER_SERVER_H

#include "../include/raft.h"
#include "../include/raft/uv.h"

struct Fsm
{
    unsigned long long count;
};

struct Server;
typedef void (*ServerCloseCb)(struct Server *server);

struct Server
{
    void *data;                         /* User data context. */
    struct uv_loop_s *loop;             /* UV loop. */
                                        //    struct uv_timer_s timer;            /* To periodically apply a new entry. */
    const char *dir;                    /* Data dir of UV I/O backend. */
    struct raft_uv_transport transport; /* UV I/O backend transport. */
    struct raft_io io;                  /* UV I/O backend. */
    struct raft_fsm fsm;                /* Sample application FSM. */
    unsigned id;                        /* Raft instance ID. */
    char *address;                      /* Raft instance address. */
    struct raft raft;                   /* Raft instance. */
    struct raft_transfer transfer;      /* Transfer leadership request. */
    ServerCloseCb close_cb;             /* Optional close callback. */
};

int serverProcStart(const char* dir, unsigned id, struct Server *server, struct raft_configuration configuration);
