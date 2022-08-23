//
// Created by yjzzjy4 on 8/18/22.
//

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "cli.h"
#include "../src/configuration.h"

#define INVALID_ARGUMENTS 1

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

static void forkServer(const char *topLevelDir,
                       unsigned id,
                       struct chunk_server *chunkServer,
                       struct raft_configuration configuration)
{
    chunkServer->pid = fork();
    if (chunkServer->pid == 0) {
        char *dir = malloc(strlen(topLevelDir) + strlen("/D") + 1);
        int rv;
        sprintf(dir, "%s/%u", topLevelDir, id);
        rv = ensureDir(dir);
        if (rv != 0) {
            goto err;
        }
        rv = serverProcStart(dir, id, &chunkServer->server, configuration);
        if (rv != 0) {
            goto err;
        }
        chunkServer->status = ALIVE;
        return;

    err:
        chunkServer->status = UNAVAILABLE;
        abort();
    }
}

/**
 * Generate a raft_server struct and append it to the given configuration (with the given role).
 *
 * @param id
 * @param configuration
 * @param role
 */
static void bootstrap_configuration(unsigned id, struct raft_configuration *configuration, int role) {
    char address[64];
    sprintf(address, "127.0.0.1:900%d", id);
    raft_configuration_add(configuration, id, address, role);
}

void command_init(struct command *cmd)
{
    assert(cmd != NULL);
    char **args = (char **) malloc(10 * sizeof(char *));
    cmd->args = args;
    cmd->size = 10;
    cmd->n = 0;
}

void command_free(struct command *cmd)
{
    if (cmd == NULL) {
        return;
    }
    free(cmd->args);
    free(cmd);
    cmd = NULL;
}

void parse_command(char *input, struct command *cmd)
{
    command_init(cmd);
    char *token = strtok(input, " ");
    while (token != NULL) {
        if (cmd->n >= cmd->size) {
            int newSize = cmd->size + cmd->size / 2;
            cmd->args = (char **) realloc(cmd->args, newSize * sizeof(char *));
            cmd->size = newSize;
        }
        cmd->args[cmd->n++] = token;
        token = strtok(NULL, " ");
    }
}

int dispatch(char *input)
{
    struct command *cmd = (struct command *) malloc(sizeof(struct command));
    parse_command(input, cmd);

    int rv = -1;

    if (strcmp(cmd->args[0], "kill") == 0) {
        unsigned id = atoi(cmd->args[1]);
        if (id == 0) {
            return INVALID_ARGUMENTS;
        }
        rv = exec_kill(id);
    }
    else if (strcmp(cmd->args[0], "add") == 0) {
        unsigned id = atoi(cmd->args[1]);
        if (id == 0) {
            return INVALID_ARGUMENTS;
        }
        rv = exec_add(id);
    }
//    else if (strcmp(cmd->args[0], "loggeradd") == 0) {
//        rv = exec_logger_add(0);
//    }
    else if (strcmp(cmd->args[0], "disconnect") == 0) {
        unsigned id = atoi(cmd->args[1]);
        if (id == 0) {
            return INVALID_ARGUMENTS;
        }
        rv = exec_disconnect(id);
    }
    else if (strcmp(cmd->args[0], "reconnect") == 0) {
        unsigned id = atoi(cmd->args[1]);
        if (id == 0) {
            return INVALID_ARGUMENTS;
        }
        rv = exec_reconnect(id);
    }
    else if (strcmp(cmd->args[0], "pause") == 0) {
        rv = exec_pause();
    }
    else if (strcmp(cmd->args[0], "resume") == 0) {
        rv = exec_resume();
    }
    else if (strcmp(cmd->args[0], "transfer") == 0) {
        unsigned id = atoi(cmd->args[1]);
        if (id == 0) {
            return INVALID_ARGUMENTS;
        }
        rv = exec_leadership_transfer(0);
    }

    command_free(cmd);
    cmd = NULL;

    return rv;
}

void configuration_change_cb(struct raft_change *req, int status) {
    // todo: apply configuration change or abort.
}

int exec_kill(unsigned id) {
    struct chunk_server *cs;
    int rv = get_chunk_server(id, &cs);
    if (rv != 0) {
        return rv;
    }
    rv = kill(cs->pid, SIGINT);
    if (rv != 0) {
        printf("error: kill server %d: %s", id, strerror(errno));
        return -1;
    }
    cs->pid = -1;
    cs->status = DEAD;
    // todo: maybe start a logger automatically to replace server(id)?
    return rv;
}

int exec_add(unsigned id) {
    struct chunk_server *cs;
    int rv = get_chunk_server(id, &cs);
    if (rv == 0) {
        return -1;
    }
    rv = get_first_unavailable_chunk_server(&cs);
    if (rv != 0) {
        return -1;
    }

    // Start server.
    struct raft_configuration configuration;
    raft_configuration_init(&configuration);
    bootstrap_configuration(id, &configuration, RAFT_VOTER);
    forkServer(top_level_dir, id, cs, configuration);
    if (cs->status != ALIVE) {
        printf("error: start server %d\n", id);
        return -1;
    }

    // Prepare to make configuration change.
    struct raft_configuration newConfig;
    configurationCopy(&cluster_map.configuration, &newConfig);
    raft_configuration_add(&newConfig, cs->server.id, cs->server.address, RAFT_VOTER);
    uncommitted_config = &newConfig;
    struct raft_server *servers = cluster_map.configuration.servers;
    for (int i = 0; i < cluster_map.configuration.n; i++) {
        struct chunk_server *chunkServer;
        get_chunk_server(servers[i].id, &chunkServer);
        if (chunkServer->status == ALIVE) {
            struct raft_change req;
            rv = raft_add(&chunkServer->server.raft, &req, cs->server.id, cs->server.address, configuration_change_cb);
            if (rv == RAFT_NOTLEADER) {
                continue;
            }
            if (rv != 0) {
                raft_configuration_close(uncommitted_config);
                uncommitted_config = NULL;
                return rv;
            }
        }
    }
    // No leader?
    raft_configuration_close(uncommitted_config);
    uncommitted_config = NULL;
    return -1;
}

int main(int argc, char *argv[])
{
    struct timespec now;
    unsigned i;
    int rv;

    if (argc > 2) {
        printf("usage: example-cluster [<dir>]\n");
        return 1;
    }

    if (argc == 2) {
        top_level_dir = argv[1];
    }

    /* Make sure the top level directory exists. */
    rv = ensureDir(top_level_dir);
    if (rv != 0) {
        return rv;
    }

    /* Initialize cluster configuration. */
    raft_configuration_init(&cluster_map.configuration);
    for (i = 0; i < N_SERVERS; i++) {
        bootstrap_configuration(i + 1, &cluster_map.configuration, RAFT_VOTER);
    }

    /* Initialize cluster chunk servers. */
    cluster_map.chunkServers = malloc(MAX_N_SERVERS * sizeof (struct chunk_server));
    memset(cluster_map.chunkServers, 0, MAX_N_SERVERS * sizeof (struct chunk_server));
    cluster_map.size = MAX_N_SERVERS;

    /* Spawn the cluster chunk servers */
    for (i = 0; i < cluster_map.configuration.n; i++) {
        struct chunk_server cs = cluster_map.chunkServers[i];
        forkServer(top_level_dir, cluster_map.configuration.servers[i].id, &cs, cluster_map.configuration);
        if (cs.status != ALIVE) {
            for (int j = 0; j < i; j++) {
                rv = kill(cluster_map.chunkServers[j].pid, SIGINT);
                if (rv != 0) {
                    printf("error: kill server %d: %s", i, strerror(errno));
                }
            }
            return 1;
        }
    }

    /* Seed the random generator */
    timespec_get(&now, TIME_UTC);
    srandom((unsigned)(now.tv_nsec ^ now.tv_sec));

    // todo: set up a timer to send entries periodically to cluster.

    while (1) {

        char *input = NULL;
        size_t len = 0;

        getline(&input, &len, stdin);

        rv = dispatch(input);

        printf("%d\n", rv);
    }

    return 0;
}
