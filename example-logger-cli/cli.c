//
// Created by Administrator on 2022/9/9.
//
// SPDX-License-Identifier: Proprietary
/*
 *   Yunhe Enmo (Beijing) Information Technology Co., Ltd.
 *   Copyright (c) 2021 , All rights reserved.
 *
 */
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "cli.h"

#define _pl()                                                            \
    printf("file=%s, line=%d, func=%s, g_status=%d, cons=%d, prod=%d\n", \
           __FILE__, __LINE__, __func__, g_status, g_out.consume_ptr,    \
           g_out.produce_ptr)

#define LOG_ERR printf

#define LINE_LEN 256
#define ARG_NUM (LINE_LEN / 2)
#define FUNC_NAME_LEN 32
#define FUNC_HELP_LEN 128
#define FUNC_NUM 128
#define Q_LEN 32

/* CLI states. */
#define CLI_INIT 0
#define CLI_WAIT 1
#define CLI_SESSION 2
#define CLI_TALK 3
#define CLI_TERM 4
#define CLI_ERROR 5

struct cli_cmd_s
{
    struct cli_cmd_s
        *next; /* Points to the next command with the same hash value. */
    char name[FUNC_NAME_LEN];             /* Command name. */
    char help[FUNC_HELP_LEN];             /* Command help (description). */
    void (*func)(int argc, char *argv[]); /* Command function. */
};

//output queue，这相当于循环队列，consumer和producer相互追赶
struct queue_s
{
    char *bufs[Q_LEN];
    int consume_ptr, produce_ptr;
};

static struct cli_cmd_s
    g_func[FUNC_NUM];  /* Stores each command in adding sequence. */
static int g_func_num; /* Indicates how many command are registered. */

static struct cli_cmd_s
    *g_hash[FUNC_NUM]; /* Hash table for finding command by fullname. */

static char g_sock_path[PATH_MAX];
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;
static int g_status;
static pthread_t g_cli_pth;
static int g_conn; /* CLI socket connection file descriptor. */

static struct queue_s g_out; /* Output queue. */

static int q_empty(struct queue_s *q)
{
    return q->consume_ptr == q->produce_ptr;
}

static int q_full(struct queue_s *q)
{
    int next = (q->produce_ptr + 1) % Q_LEN;
    return next == q->consume_ptr;
}

static int q_enq(struct queue_s *q, char *buf)
{
    int next = (q->produce_ptr + 1) % Q_LEN;

    if (q_full(q))
        return -ENOMEM;
    q->bufs[q->produce_ptr] = buf;
    q->produce_ptr = next;
    return 0;
}

static char *q_deq(struct queue_s *q)
{
    int next = (q->consume_ptr + 1) % Q_LEN;
    char *buf;

    if (q_empty(q))
        return NULL;
    buf = q->bufs[q->consume_ptr];
    q->consume_ptr = next;
    return buf;
}

static void cli_lock(void)
{
    pthread_mutex_lock(&g_lock);
}

static void cli_unlock(void)
{
    pthread_mutex_unlock(&g_lock);
}

/**
 * Generate a hash value for str.
 *
 * @param str
 *
 * @return hash value.
 */
static unsigned int str_hash(char *str)
{
    /*
     * The magic number doesn't have any meaning, so that we don't
     * define a new constant for it. It should be a prime number.
     */
    unsigned int v = 5381;
    char *p = str;

    while (*p) {
        v *= 33;
        v += (unsigned int)*p;
        p++;
    }
    return v;
}

/**
 * Find a command by full name in g_hash.
 *
 * @param cmd       command fullname;
 * @param ret_hash  hash value for cmd.
 *
 * @return Pointer to target command.
 */
static struct cli_cmd_s *cli_find_cmd(char *cmd, unsigned int *ret_hash)
{
    unsigned int v;
    struct cli_cmd_s *p;
    //先找hash值，然后遍历链表
    v = str_hash(cmd) % FUNC_NUM;
    if (ret_hash)
        *ret_hash = v;

    for (p = g_hash[v]; p != NULL; p = p->next) {
        if (strcmp(p->name, cmd) == 0)
            return p;
    }
    return NULL;
}
//不清楚用途
static void drain_q(void)
{
    char *line;
    int cmd_end = 0;
    int n, ret;

    while (!q_empty(&g_out)) {
        line = q_deq(&g_out);
        if (cmd_end) {
            free(line);
            continue;
        }

        if (strcmp(line, "[cmd_end]") == 0) {
            cmd_end = 1;
            g_status = CLI_SESSION;
            free(line);
            continue;
        }

        n = strlen(line);
        ret = write(g_conn, line, n);
        if (ret < 0) {
            cmd_end = 1;
            g_status = CLI_WAIT;
        }
        free(line);
    }
}

int cli_printf(char *fmt, ...)
{
    char *line;
    va_list aptr;
    int ret;

    if (pthread_self() == g_cli_pth) {
        cli_lock();
        drain_q();
        cli_unlock();
    }

    line = (char *)malloc(LINE_LEN);
    if (line == NULL) {
        LOG_ERR("out of memory\n");
        return -ENOMEM;
    }

    va_start(aptr, fmt);
    ret = vsnprintf(line, LINE_LEN, fmt, aptr);
    va_end(aptr);
    ret = ret < LINE_LEN ? ret : LINE_LEN - 1;
    line[ret] = '\0';
    cli_lock();
    q_enq(&g_out, line);
    pthread_cond_signal(&g_cond);
    cli_unlock();

    return ret;
}

/**
 * Register new command.
 *
 * @param cmd   command fullname;
 * @param help  command's help info;
 * @param func  command function.
 *
 * @return zero on success, -EEXIST if command already existed, -ENOMEM if
 * memory limit exceeded.
 */
int cli_reg_cmd(char *cmd, char *help, void (*func)(int, char **))
{
    unsigned int v;
    struct cli_cmd_s *p;

    cli_lock();
    p = cli_find_cmd(cmd, &v);
    if (p != NULL) {
        cli_unlock();
        return -EEXIST;
    }

    if (g_func_num >= FUNC_NUM) {
        cli_unlock();
        return -ENOMEM;
    }

    p = &g_func[g_func_num];
    g_func_num++;

    p->next = g_hash[v];
    strncpy(p->name, cmd, FUNC_NAME_LEN);
    p->name[FUNC_NAME_LEN - 1] = '\0';
    strncpy(p->help, help, FUNC_NAME_LEN);
    p->help[FUNC_HELP_LEN - 1] = '\0';
    p->func = func;
    g_hash[v] = p;

    cli_unlock();
    return 0;
}

void cli_cmd_end(void)
{
    cli_lock();
    q_enq(&g_out, strdup("[cmd_end]"));
    pthread_cond_signal(&g_cond);
    cli_unlock();
}

static void cli_run_cmd(int argc, char *argv[])
{
    struct cli_cmd_s *p;
    const char *exit_str = "\ncommand finished\n";
    int ret;

    p = cli_find_cmd(argv[0], NULL);
    if (p == NULL) {
        const char *unknown = "unknown command\n";

        ret = write(g_conn, unknown, strlen(unknown));
        if (ret < 0)
            g_status = CLI_WAIT;
        else
            g_status = CLI_SESSION;
        return;
    }

    cli_unlock();
    p->func(argc, argv);
    cli_lock();

    while (g_status == CLI_TALK || !q_empty(&g_out)) {
        while (g_status == CLI_TALK && q_empty(&g_out))
            pthread_cond_wait(&g_cond, &g_lock);
        drain_q();
    }
    if (g_status == CLI_SESSION) {
        ret = write(g_conn, exit_str, strlen(exit_str));
        if (ret < 0)
            g_status = CLI_WAIT;
    }
}

/* split line into argv[] */
static void line2argv(char *line, int *argc, char *argv[])
{
    char *p = line;
    int spc = 1;

    *argc = 0;
    line[LINE_LEN - 1] = '\0';
    while (*p != '\0') {
        switch (*p) {
            case ' ':
            case '\t':
                *p = '\0';
                spc = 1;
                break;
            default:
                if (spc) {
                    argv[*argc] = p;
                    *argc += 1;
                    spc = 0;
                }
                break;
        }
        p++;
    }
}

static int ready_to_read(fd_set *fs, int fd)
{
    struct timeval tv;
    int ret;

    FD_SET(fd, fs);
    tv.tv_sec = 0;
    tv.tv_usec = 1000;
    cli_unlock();
    ret = select(fd + 1, fs, NULL, NULL, &tv);
    cli_lock();
    if (ret < 0) {
        LOG_ERR("select, errno=%d/%s\n", errno, strerror(errno));
        return ret;
    }

    return ret;
}

/**
 * CLI reads from socket file descriptor, parses and executes received commands.
 *
 * @param connfd
 */
static void cli_serve_client(int connfd)
{
    int n, ret;
    fd_set rs;
    static char line[LINE_LEN];
    static char *argv[ARG_NUM];
    int argc;

    g_conn = connfd;
    FD_ZERO(&rs);
    while (g_status == CLI_SESSION) {
        ret = ready_to_read(&rs, connfd);
        if (ret < 0) {
            g_status = CLI_WAIT;
            return;
        }

        cli_unlock();
        n = read(connfd, line, sizeof(line) - 1);
        cli_lock();
        if (n <= 0) {
            g_status = CLI_WAIT;
            return;
        }
        line[n] = '\0';
        line2argv(line, &argc, argv);
        g_status = CLI_TALK;
        cli_run_cmd(argc, argv);
    }
    g_conn = -1;
    close(connfd);
}

/**
 * CLI accepts data from client through listen file descriptor.
 *
 * @param listenfd
 */
static void cli_server_accept(int listenfd)
{
    socklen_t len;
    struct sockaddr_un cliun;
    int connfd;
    fd_set rs;
    int ret;

    FD_ZERO(&rs);

    while (g_status == CLI_WAIT) {
        ret = ready_to_read(&rs, listenfd);
        if (ret < 0) {
            g_status = CLI_ERROR;
            break;
        } else if (ret == 0)
            continue;

        len = sizeof(cliun);
        cli_unlock();
        connfd = accept(listenfd, (struct sockaddr *)&cliun, &len);
        cli_lock();
        if (connfd < 0) {
            LOG_ERR("accept error, errno=%d/%s\n", errno, strerror(errno));
            g_status = CLI_ERROR;
            break;
        }
        g_status = CLI_SESSION;
        cli_serve_client(connfd);
    }
    close(listenfd);
    if (g_status != CLI_TERM)
        LOG_ERR("g_status = %d\n", g_status);
    else
        g_status = CLI_INIT;
}

static void *cli_server_func(void *arg)
{
    struct sockaddr_un addr;
    int listenfd, size;

    cli_lock();
    listenfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listenfd < 0) {
        g_status = CLI_ERROR;
        LOG_ERR("socket, errno=%d/%s\n", errno, strerror(errno));
        cli_unlock();
        return NULL;
    }

    unlink(g_sock_path);

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, g_sock_path);
    size = offsetof(struct sockaddr_un, sun_path) + strlen(addr.sun_path);
    if (bind(listenfd, (struct sockaddr *)&addr, size) < 0) {
        g_status = CLI_ERROR;
        LOG_ERR("bind, errno=%d/%s\n", errno, strerror(errno));
        cli_unlock();
        return NULL;
    }

    if (listen(listenfd, 1) < 0) {
        g_status = CLI_ERROR;
        LOG_ERR("listen, errno=%d/%s\n", errno, strerror(errno));
        cli_unlock();
        return NULL;
    }

    cli_server_accept(listenfd);

    cli_unlock();
    return NULL;
}

static void help_func(int argc, char **argv)
{
    int i;
    struct cli_cmd_s *p;

    for (i = 0; i < g_func_num; i++) {
        p = &g_func[i];
        cli_printf("%20s - %s\n", p->name, p->help);
    }
    cli_cmd_end();
}

void init_cli_server(const char *sock_path)
{
    int ret;

    cli_lock();

    strncpy(g_sock_path, sock_path, PATH_MAX);
    g_sock_path[PATH_MAX - 1] = '\0';

    if (g_status != CLI_INIT) {
        LOG_ERR("g_status = %d\n", g_status);
        cli_unlock();
        return;
    }

    g_status = CLI_WAIT;
    ret = pthread_create(&g_cli_pth, NULL, cli_server_func, NULL);
    if (ret < 0) {
        LOG_ERR("pthread_create, ret = %d\n", ret);
        cli_unlock();
        return;
    }
    cli_unlock();
    cli_reg_cmd("help", "Show this messages", help_func);
}

void exit_cli_server(void)
{
    cli_lock();
    if (g_status != CLI_WAIT && g_status != CLI_SESSION &&
        g_status != CLI_TALK) {
        LOG_ERR("g_status = %d\n", g_status);
        cli_unlock();
        return;
    }
    g_status = CLI_TERM;
    cli_unlock();
    pthread_join(g_cli_pth, NULL);
}
