//
// Created by yjzzjy4 on 8/18/22.
//

#ifndef RAFT_CLI_H
#define RAFT_CLI_H

#endif  // RAFT_CLI_H

#include "../include/raft.h"
#include "another_server.h"

#define N_SERVERS 3                      /* Number of servers in the example cluster */
#define MAX_N_SERVERS 9                  /* Max number of servers started */
const char *top_level_dir = "/tmp/raft"; /* Default directory to store persisted fsm data */

struct command
{
    char **args;
    int size;
    int n;
};

enum server_status { UNAVAILABLE, ALIVE, DEAD, DISCONNECTED };

/**
 * Hold a struct for a chunk server.
 */
struct chunk_server
{
    pid_t pid;
    struct Server server;
    enum server_status status;
};

/**
 * Hold a struct for the running cluster.
 */
struct cluster_map
{
    struct chunk_server *chunkServers;          /* All servers */
    struct raft_configuration configuration;    /* Current running configuration */
    int size;                                   /* How many servers are in chunkServer */
} cluster_map;

struct raft_configuration *uncommitted_config;

void command_init(struct command *cmd);
void command_free(struct command *cmd);
void parse_command(char *input, struct command *cmd);

int exec_kill(unsigned id);
int exec_add(unsigned id);
//int exec_logger_add(unsigned id);
int exec_disconnect(unsigned id);
int exec_reconnect(unsigned id);
int exec_pause();
int exec_resume();
int exec_leadership_transfer(unsigned id);

/**
 * Dispatch a command to be executed properly.
 *
 * @param input user input command string.
 * @param cluster_map topology of the target cluster.
 *
 * @return Zero if succeeded, otherwise non-zero.
 */
int dispatch(char *input);

/**
 * Get a chunk server from cluster map.
 *
 * @param id
 * @param chunkServer
 *
 * @return
 */
int get_chunk_server(int id, struct chunk_server **chunkServer)
{
    for (int i = 0; i < cluster_map.size; i++) {
        struct chunk_server cs = cluster_map.chunkServers[i];
        if (cs.server.id == id) {
            *chunkServer = &cs;
            return 0;
        }
    }
    return -1;
}

/**
 * Get a invalid chunk server from cluster map.
 *
 * @param id
 * @param chunkServer
 *
 * @return
 */
int get_first_unavailable_chunk_server(struct chunk_server **chunkServer)
{
    for (int i = 0; i < cluster_map.size; i++) {
        struct chunk_server cs = cluster_map.chunkServers[i];
        if (cs.status == UNAVAILABLE) {
            *chunkServer = &cs;
            return 0;
        }
    }
    return -1;
}
