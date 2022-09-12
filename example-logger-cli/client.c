//
// Created by yjzzjy4 on 8/31/22.
//
#include <errno.h>
#include <linux/limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <uv.h>

#include "cli.h"

#define N_SERVERS 3   /* Number of servers in the cluster */
#define SERVER_NUM 10 /* Max Number of servers in the cluster */

#define SERVERS_NO_ROOM 1

typedef unsigned long long u_ll;

enum server_state { FREE, INUSE };

struct server_info
{
    u_ll id;
    pid_t pid;
    char *address;
    char *socket_path;
    enum server_state state;
};

struct server_info cluster_info[SERVER_NUM];

int server_num;
char *socket_path = "/tmp/cluster-client.sock";

static int getFreeServer(struct server_info **server)
{
    if (server_num >= SERVER_NUM) {
        goto no_room;
    }

    for (int i = 0; i < SERVER_NUM; i++) {
        if (cluster_info[i].state == FREE) {
            *server = &cluster_info[i];
            return 0;
        }
    }

no_room:
    *server = NULL;
    return SERVERS_NO_ROOM;
}

static int ensureDir(const char *dir)
{
    int rv;
    struct stat sb;
    rv = stat(dir, &sb);
    if (rv == -1) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                printf("error: create directory '%s': %s", dir,
                       strerror(errno));
                return 1;
            }
        } else {
            printf("error: stat directory '%s': %s", dir, strerror(errno));
            return 1;
        }
    } else {
        if ((sb.st_mode & S_IFMT) != S_IFDIR) {
            printf("error: path '%s' is not a directory", dir);
            return 1;
        }
    }
    return 0;
}

static void forkServer(const char *topLevelDir, u_ll i, struct server_info *serverInfo)
{
    serverInfo->pid = fork();
    if (serverInfo->pid == 0) {
        char *dir = malloc(strlen(topLevelDir) + strlen("/D") + 1);
        char *id = malloc(N_SERVERS / 10 + 2);
        char address[64];
        char socketPath[PATH_MAX];
        char *argv[] = {"./example-logger-cli/server", dir, id, address, socketPath, NULL};
        char *envp[] = {NULL};
        int rv;
        sprintf(dir, "%s/%llu", topLevelDir, i + 1);
        rv = ensureDir(dir);
        if (rv != 0) {
            abort();
        }
        sprintf(id, "%llu", i + 1);
        sprintf(address, "127.0.0.1:900%s", id);
        sprintf(socketPath, "/tmp/chunk-server-%s.sock", id);
        serverInfo->state = INUSE;
        serverInfo->id = i + 1;
        serverInfo->address = address;
        serverInfo->socket_path = socketPath;
        execve("./example-logger-cli/server", argv, envp);
    }
}

/* Called periodically every APPLY_RATE milliseconds. */
static void serverTimerCb(uv_timer_t *timer)
{
    // todo send 'apply' command to servers periodically;
    // foreach server:
    //    write(socketfs, "apply", 100);
}

static void addFunc(int argc, char* argv[]) {
    // todo redirect to cluster servers;
}

int main(int argc, char *argv[])
{
    const char *topLevelDir = "/tmp/raft";
    struct timespec now;
    pid_t pids[N_SERVERS];
    u_ll i;
    int rv;

    if (argc > 2) {
        printf("usage: example-cluster [<dir>]\n");
        return 1;
    }

    if (argc == 2) {
        topLevelDir = argv[1];
    }

    /* Make sure the top level directory exists. */
    rv = ensureDir(topLevelDir);
    if (rv != 0) {
        return rv;
    }

    /* Spawn the cluster nodes */
    for (i = 0; i < N_SERVERS; i++) {
        struct server_info *serverInfo;
        rv = getFreeServer(&serverInfo);
        if (serverInfo == NULL) {
            return rv;
        }
        forkServer(topLevelDir, i, serverInfo);
    }

    init_cli_server(socket_path);
    cli_reg_cmd("create-cluster", "", NULL);
    cli_reg_cmd("add", "", addFunc);
    cli_reg_cmd("kill", "", NULL);
    cli_reg_cmd("remove", "", NULL);

    /* Seed the random generator */
    timespec_get(&now, TIME_UTC);
    srandom((unsigned)(now.tv_nsec ^ now.tv_sec));

    while (1) {
//        struct timespec interval;
//        int status;
//
//        /* Sleep a little bit. */
//        interval.tv_sec = 1 + random() % 15;
//        interval.tv_nsec = 0;
//
//        rv = nanosleep(&interval, NULL);
//        if (rv != 0) {
//            printf("error: sleep: %s", strerror(errno));
//        }
//
//        /* Kill a random server. */
//        i = (unsigned)(random() % N_SERVERS);
//
//        rv = kill(pids[i], SIGINT);
//        if (rv != 0) {
//            printf("error: kill server %d: %s", i, strerror(errno));
//        }
//
//        waitpid(pids[i], &status, 0);
//
//        rv = nanosleep(&interval, NULL);
//        if (rv != 0) {
//            printf("error: sleep: %s", strerror(errno));
//        }
//
//        forkServer(topLevelDir, i, &pids[i]);
    }

    return 0;
}
