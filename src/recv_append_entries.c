#include "recv_append_entries.h"

#include <sys/syscall.h> /*必须引用这个文件 */
#include "assert.h"
#include "byte.h"
#include "convert.h"
#include "entry.h"
#include "heap.h"
#include "log.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"

#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
static int getRaftRole(struct raft *r)
{
    for (unsigned i = 0; i < r->configuration.n; i++) {
        if (r->id == r->configuration.servers[i].id) {
            return r->configuration.servers[i].role;
        }
    }
    //TODO BAD_ROLE
    return -1;
}
static void recvSendAppendEntriesResultCb(struct raft_io_send *req, int status)
{
    (void)status;
    RaftHeapFree(req);
}

// TODO logger接收entries的逻辑！
int recvLoggerAppendEntries(struct raft *r,
                            raft_id id,
                            const char *address,
                            const struct raft_append_entries *args)
{
    printf("recvLoggerAppendEntries call\n");
    struct raft_io_send *req;
    struct raft_message message;
    struct raft_append_entries_result *result = &message.append_entries_result;
    int match;
    bool async;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);
    assert(address != NULL);
    tracef(
        "self:%llu from:%llu@%s leader_commit:%llu n_entries:%d "
        "prev_log_index:%llu prev_log_term:%llu, term:%llu",
        r->id, id, address, args->leader_commit, args->n_entries,
        args->prev_log_index, args->prev_log_term, args->term);

    result->rejected = args->prev_log_index;
    result->last_log_index = logLastIndex(&r->log);

    rv = recvEnsureMatchingTerms(r, args->term, &match);
    if (rv != 0) {
        printf("recvEnsureMatchingTerms rv %d\n", rv);
        return rv;
    }
    // match 为
    // 1的含义？recvEnsureMatchingTerms调recvCheckMatchingTerms，返回1，说明logger的term小于leader的term
    //第一次接收到entry的时候，说明
    //可以在这里给改，主要是扩容
    printf("ensure match %d\n", match);
    printf("logger log offset %d, refs_size %d front %d back %d size %d\n",
           r->log.offset, r->log.refs_size, r->log.front, r->log.back,
           r->log.size);
    printf("logger state %d current_term %d voted_for %d\n", r->state,
           r->current_term, r->voted_for);
    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: Reply false if term <
     *   currentTerm.
     */
    if (match < 0) {
        tracef("local term is higher -> reject ");
        goto reply;
    }

    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    assert(r->current_term == args->term);

    if (r->state == RAFT_CANDIDATE) {
        /* The current term and the peer one must match, otherwise we would have
         * either rejected the request or stepped down to followers. */
        assert(match == 0);
        tracef("discovered leader -> step down ");
        convertToFollower(r);
    }

    assert(r->state == RAFT_FOLLOWER);

    /* Update current leader because the term in this AppendEntries RPC is up to
     * date. */
    rv = recvUpdateLeader(r, id, address);
    printf("logger follower_state id %d, address %s\n",
           r->follower_state.current_leader.id,
           r->follower_state.current_leader.address);
    if (rv != 0) {
        return rv;
    }

    /* Reset the election timer. */
    r->election_timer_start = r->io->time(r->io);

    if (replicationInstallSnapshotBusy(r) && args->n_entries > 0) {
        tracef("ignoring AppendEntries RPC during snapshot install");
        entryBatchesDestroy(args->entries, args->n_entries);
        return 0;
    }
    printf("logger start replication\n");
    rv = loggerReplicationAppend(r, args, &result->rejected, &async);

    if (rv != 0) {
        return rv;
    }

    if (async) {
        //TODO 第一次给logger发送entries async 为 true
        return 0;
    }

    /* Echo back to the leader the point that we reached. */
    result->last_log_index = r->last_stored;

reply:

    result->term = r->current_term;

    /* Free the entries batch, if any. */
    if (args->n_entries > 0 && args->entries[0].batch != NULL) {
        raft_free(args->entries[0].batch);
    }

    if (args->entries != NULL) {
        raft_free(args->entries);
    }

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = id;
    message.server_address = address;

    req = RaftHeapMalloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->data = r;
    rv = r->io->send(r->io, req, &message, recvSendAppendEntriesResultCb);
    if (rv != 0) {
        raft_free(req);
        return rv;
    }

    return 0;
}
int recvAppendEntries(struct raft *r,
                      raft_id id,
                      const char *address,
                      const struct raft_append_entries *args)
{
    if (r->id == 4) {
        printf(
            "logger recv args n_entries %d, term %d, prev_log_index %d, "
            "leader_commit %d, prev_log_term %d\n",
            args->n_entries, args->term, args->prev_log_index,
            args->leader_commit, args->prev_log_term);
    }
    struct raft_io_send *req;
    struct raft_message message;
    struct raft_append_entries_result *result = &message.append_entries_result;
    int match;
    bool async;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);
    assert(address != NULL);

    int role = getRaftRole(r);
    if (role == -1 || role == RAFT_LOGGER) {
//        printf("args->entries[0].type %d\n",args->entries[0].type);
        return recvLoggerAppendEntries(r, id, address, args);
    }

    result->rejected = args->prev_log_index;
    result->last_log_index = logLastIndex(&r->log);

    rv = recvEnsureMatchingTerms(r, args->term, &match);
    if (rv != 0) {
        return rv;
    }

    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: Reply false if term <
     *   currentTerm.
     */
    if (match < 0) {
        tracef("local term is higher -> reject ");
        goto reply;
    }
    //    printf("voter log offset %d, refs_size %d front %d back %d size %d\n"
    //           ,r->log.offset, r->log.refs_size, r->log.front, r->log.back,
    //           r->log.size);
    /* If we get here it means that the term in the request matches our current
     * term or it was higher and we have possibly stepped down, because we
     * discovered the current leader:
     *
     * From Figure 3.1:
     *
     *   Rules for Servers: Candidates: if AppendEntries RPC is received from
     *   new leader: convert to follower.
     *
     * From Section 3.4:
     *
     *   While waiting for votes, a candidate may receive an AppendEntries RPC
     *   from another server claiming to be leader. If the leader's term
     *   (included in its RPC) is at least as large as the candidate's current
     *   term, then the candidate recognizes the leader as legitimate and
     *   returns to follower state. If the term in the RPC is smaller than the
     *   candidate's current term, then the candidate rejects the RPC and
     *   continues in candidate state.
     *
     * From state diagram in Figure 3.3:
     *
     *   [candidate]: discovers current leader -> [follower]
     *
     * Note that it should not be possible for us to be in leader state, because
     * the leader that is sending us the request should have either a lower term
     * (and in that case we reject the request above), or a higher term (and in
     * that case we step down). It can't have the same term because at most one
     * leader can be elected at any given term.
     */
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    assert(r->current_term == args->term);

    if (r->state == RAFT_CANDIDATE) {
        /* The current term and the peer one must match, otherwise we would have
         * either rejected the request or stepped down to followers. */
        assert(match == 0);
        tracef("discovered leader -> step down ");
        convertToFollower(r);
    }

    assert(r->state == RAFT_FOLLOWER);

    /* Update current leader because the term in this AppendEntries RPC is up to
     * date. */
    rv = recvUpdateLeader(r, id, address);
    if (rv != 0) {
        return rv;
    }

    /* Reset the election timer. */
    r->election_timer_start = r->io->time(r->io);

    /* If we are installing a snapshot, ignore these entries. TODO: we should do
     * something smarter, e.g. buffering the entries in the I/O backend, which
     * should be in charge of serializing everything. */
    if (replicationInstallSnapshotBusy(r) && args->n_entries > 0) {
        tracef("ignoring AppendEntries RPC during snapshot install");
        entryBatchesDestroy(args->entries, args->n_entries);
        return 0;
    }

    rv = replicationAppend(r, args, &result->rejected, &async);
    if (rv != 0) {
        return rv;
    }

    if (async) {
        return 0;
    }

    /* Echo back to the leader the point that we reached. */
    result->last_log_index = r->last_stored;
//    printf("recvAppendEntries: %5u\n", syscall(SYS_gettid));
reply:
    result->term = r->current_term;

    /* Free the entries batch, if any. */
    if (args->n_entries > 0 && args->entries[0].batch != NULL) {
        raft_free(args->entries[0].batch);
    }

    if (args->entries != NULL) {
        raft_free(args->entries);
    }

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = id;
    message.server_address = address;

    req = RaftHeapMalloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->data = r;

    rv = r->io->send(r->io, req, &message, recvSendAppendEntriesResultCb);
    if (rv != 0) {
        raft_free(req);
        return rv;
    }

    return 0;
}

#undef tracef
