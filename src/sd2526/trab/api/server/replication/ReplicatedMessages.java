package sd2526.trab.api.server.replication;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.server.java.JavaMessages;
import sd2526.trab.api.server.java.RemoteMessagesClientFactory;
import sd2526.trab.api.server.java.UsersClient;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Replicated messages service using Kafka for state machine replication.
 * Write operations are published to Kafka and applied locally only after
 * being consumed from the topic. Read operations wait until the local
 * version is at least as recent as required.
 */
public class ReplicatedMessages implements Messages {
    private static final Logger Log = Logger.getLogger(ReplicatedMessages.class.getName());
    private static final long WAIT_TIMEOUT_MS = 30_000;
    private static final long APPLY_RETRY_DELAY_MS = 1_000;

    private final JavaMessages localImpl;
    private final KafkaPublisher publisher;
    private final String topic;
    private long currentVersion;
    private final ConcurrentHashMap<String, java.util.Set<Long>> processedSidsPerDomain;

    /**
     * Constructs a {@code ReplicatedMessages} instance wired to the given
     * infrastructure dependencies.
     *
     * <p>
     * A {@link KafkaPublisher} and a {@link KafkaSubscriber} are created
     * internally. The subscriber is started immediately so this replica begins
     * applying operations from the Kafka topic before any client calls arrive.
     *
     * @param hibernate             Hibernate session factory used by the local
     *                              {@link JavaMessages} implementation
     * @param localDomainSupplier   provides the local domain name
     * @param localUsersClient      pre-built client for the local Users server
     * @param messagesClientFactory factory for clients to remote Messages servers
     * @param extraLongFaultMillis  deadline (ms) used for remote-forward retry
     *                              loops
     * @param kafkaTopic            Kafka topic that carries replication operations
     */
    public ReplicatedMessages(Hibernate hibernate,
            Supplier<String> localDomainSupplier,
            UsersClient localUsersClient,
            RemoteMessagesClientFactory messagesClientFactory,
            long extraLongFaultMillis,
            String kafkaTopic) {
        this.localImpl = new JavaMessages(hibernate, localDomainSupplier, localUsersClient,
                messagesClientFactory, extraLongFaultMillis);
        this.publisher = KafkaPublisher.createPublisher();
        this.topic = kafkaTopic;
        this.currentVersion = -1;
        this.processedSidsPerDomain = new ConcurrentHashMap<>();

        // Start consuming from Kafka to apply operations
        KafkaSubscriber subscriber = new KafkaSubscriber(kafkaTopic);
        subscriber.start(this::applyOperation);
    }

    /**
     * Returns the current version (Kafka offset) of this replica.
     */
    public synchronized long getCurrentVersion() {
        return currentVersion;
    }

    /**
     * Waits until the local replica has caught up to at least the given version.
     */
    public synchronized boolean waitForVersion(long version) {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        while (currentVersion < version) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0)
                return false;
            try {
                wait(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    /**
     * Called by the Kafka subscriber when a new operation arrives.
     */
    private void applyOperation(long offset, String value) {
        try {
            ReplicatedOperation op = ReplicatedOperation.fromJson(value);
            localImpl.setCurrentReplicationSid(offset);
            try {
                switch (op.getType()) {
                    case POST_MESSAGE -> {
                        if (op.getResolvedMid() != null) {
                            Result<String> applied = applyWithRetry(
                                    () -> localImpl.applyPost(op.getMessage(), op.getResolvedMid(),
                                            op.getResolvedPersistedDests(), op.getResolvedRemoteForwards(),
                                            op.getResolvedUnknownDests(), op.getResolvedSenderAddress()));
                            if (!applied.isOK())
                                Log.warning("POST_MESSAGE apply returned " + applied.error() + " at offset " + offset);
                        } else {
                            Result<String> applied = applyWithRetry(
                                    () -> localImpl.postMessage(op.getPwd(), op.getMessage()));
                            if (!applied.isOK())
                                Log.warning("POST_MESSAGE legacy apply returned " + applied.error() + " at offset "
                                        + offset);
                        }
                    }
                    case FORWARD_MESSAGE -> {
                        if (op.getResolvedValidForwardDests() != null) {
                            Result<List<String>> applied = applyWithRetry(
                                    () -> localImpl.applyForward(op.getMessage(), op.getResolvedValidForwardDests(),
                                            op.getResolvedUnknownForwardDests()));
                            if (!applied.isOK())
                                Log.warning(
                                        "FORWARD_MESSAGE apply returned " + applied.error() + " at offset " + offset);
                        } else {
                            Result<List<String>> applied = applyWithRetry(
                                    () -> localImpl.forwardMessage(op.getMessage()));
                            if (!applied.isOK())
                                Log.warning("FORWARD_MESSAGE legacy apply returned " + applied.error() + " at offset "
                                        + offset);
                        }
                    }
                    case REMOVE_INBOX_MESSAGE -> {
                        Result<Void> applied = op.getResolvedUserAddress() != null
                                ? applyWithRetry(() -> localImpl.applyRemoveInboxMessage(op.getResolvedUserAddress(),
                                        op.getMessageId()))
                                : applyWithRetry(() -> localImpl.removeInboxMessage(op.getUserName(), op.getMessageId(),
                                        op.getPwd()));
                        if (!applied.isOK())
                            Log.warning(
                                    "REMOVE_INBOX_MESSAGE apply returned " + applied.error() + " at offset " + offset);
                    }
                    case DELETE_MESSAGE -> {
                        Result<Void> applied = op.getResolvedUserAddress() != null
                                ? applyWithRetry(() -> localImpl.applyDeleteMessage(op.getResolvedUserAddress(),
                                        op.getMessageId()))
                                : applyWithRetry(() -> localImpl.deleteMessage(op.getUserName(), op.getMessageId(),
                                        op.getPwd()));
                        if (!applied.isOK())
                            Log.warning("DELETE_MESSAGE apply returned " + applied.error() + " at offset " + offset);
                    }
                    case DELETE_PROPAGATED_MESSAGE -> {
                        Result<Void> applied = applyWithRetry(
                                () -> localImpl.deletePropagatedMessage(op.getMessageId()));
                        if (!applied.isOK())
                            Log.warning("DELETE_PROPAGATED_MESSAGE apply returned " + applied.error() + " at offset "
                                    + offset);
                    }
                    // SYNC is a no-op marker published only to advance the Kafka offset
                    // so that read operations can wait for a known version.
                    case SYNC -> {
                    }
                }
            } finally {
                localImpl.setCurrentReplicationSid(-1);
            }
        } catch (Exception e) {
            Log.warning("Failed to apply operation at offset " + offset + ": " + e.getMessage());
        }

        synchronized (this) {
            currentVersion = offset;
            notifyAll();
        }
    }

    /**
     * Executes {@code operation} and returns its result; if the result is a
     * transient error ({@code INTERNAL_ERROR} or {@code TIMEOUT}) the call is
     * retried after {@link #APPLY_RETRY_DELAY_MS} milliseconds. The loop
     * continues indefinitely until a terminal result (OK or permanent error) is
     * returned.
     *
     * <p>
     * This method is called during Kafka apply-phase so that transient
     * persistence failures do not permanently lose an operation.
     *
     * @param <T>       result value type
     * @param operation the operation to execute
     * @return the first terminal {@link Result}
     */
    private <T> Result<T> applyWithRetry(Supplier<Result<T>> operation) {
        while (true) {
            Result<T> res = operation.get();
            if (res.isOK())
                return res;

            if (res.error() != Result.ErrorCode.INTERNAL_ERROR && res.error() != Result.ErrorCode.TIMEOUT)
                return res;

            try {
                Thread.sleep(APPLY_RETRY_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Result.error(Result.ErrorCode.INTERNAL_ERROR);
            }
        }
    }

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        // Phase 1: authenticate and resolve all destinations (Users service call here
        // only)
        Result<JavaMessages.PostResolution> phase1 = localImpl.resolvePost(pwd, msg);
        if (!phase1.isOK())
            return Result.error(phase1);

        JavaMessages.PostResolution res = phase1.value();

        // Publish to Kafka with pre-resolved data so phase 2 is deterministic
        ReplicatedOperation op = ReplicatedOperation.postMessage(pwd, msg,
                res.mid, res.persistedDestinations, res.remoteForwardsByDomain,
                res.unknownDests, res.senderAddress);
        long offset = publisher.publish(topic, op.toJson());
        if (offset < 0)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);

        if (!waitForVersion(offset))
            return Result.error(Result.ErrorCode.TIMEOUT);
        return Result.ok(res.mid);
    }

    /**
     * Forwards an incoming message from a remote domain to local recipients.
     * Delegates to {@link #forwardMessage(Message, Long, String)} with {@code null}
     * deduplication metadata (no SID-based duplicate detection).
     *
     * @param msg the message to forward
     * @return list of unknown local recipients, or an error result
     */
    @Override
    public Result<List<String>> forwardMessage(Message msg) {
        return forwardMessage(msg, null, null);
    }

    /**
     * Forwards an incoming message from a remote domain with optional
     * SID-based deduplication. When {@code sid} and {@code sourceDomain} are
     * non-null the call is a no-op if the same SID from that domain was already
     * processed, preventing duplicate deliveries during replica catch-up.
     *
     * @param msg          message to store for local recipients
     * @param sid          Kafka offset of the originating operation, or
     *                     {@code null}
     * @param sourceDomain sending domain, or {@code null}
     * @return list of unknown local recipients, or an error result
     */
    public Result<List<String>> forwardMessage(Message msg, Long sid, String sourceDomain) {
        if (sid != null && sourceDomain != null) {
            if (registerSidIfNew(sourceDomain, sid)) {
                Log.info("Discarding duplicate forwardMessage from " + sourceDomain + " sid=" + sid);
                return Result.ok(List.of());
            }
        }

        // Phase 1: resolve local destinations (Users service call here only)
        Result<JavaMessages.ForwardResolution> phase1 = localImpl.resolveForward(msg);
        if (!phase1.isOK())
            return Result.error(phase1);

        JavaMessages.ForwardResolution res = phase1.value();

        ReplicatedOperation op = ReplicatedOperation.forwardMessage(msg, sid, sourceDomain,
                res.validLocalDests, res.unknownLocalDests);
        long offset = publisher.publish(topic, op.toJson());
        if (offset < 0)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);

        if (!waitForVersion(offset))
            return Result.error(Result.ErrorCode.TIMEOUT);
        return Result.ok(res.unknownLocalDests);
    }

    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        long syncOffset = publisher.publish(topic, ReplicatedOperation.sync().toJson());
        if (syncOffset >= 0 && !waitForVersion(syncOffset)) {
            return Result.error(Result.ErrorCode.TIMEOUT);
        }
        return localImpl.getInboxMessage(name, mid, pwd);
    }

    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        long syncOffset = publisher.publish(topic, ReplicatedOperation.sync().toJson());
        if (syncOffset >= 0 && !waitForVersion(syncOffset)) {
            return Result.error(Result.ErrorCode.TIMEOUT);
        }
        return localImpl.getAllInboxMessages(name, pwd);
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Result<String> phase1 = localImpl.resolveRemoveInbox(name, pwd, mid);
        if (!phase1.isOK())
            return Result.error(phase1);

        ReplicatedOperation op = ReplicatedOperation.removeInboxMessage(name, mid, pwd, phase1.value());
        long offset = publisher.publish(topic, op.toJson());
        if (offset < 0)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        if (!waitForVersion(offset))
            return Result.error(Result.ErrorCode.TIMEOUT);
        return Result.ok();
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Result<String> phase1 = localImpl.resolveDeleteMessage(name, pwd, mid);
        if (!phase1.isOK())
            return Result.error(phase1);

        ReplicatedOperation op = ReplicatedOperation.deleteMessage(name, mid, pwd, phase1.value());
        long offset = publisher.publish(topic, op.toJson());
        if (offset < 0)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        if (!waitForVersion(offset))
            return Result.error(Result.ErrorCode.TIMEOUT);
        return Result.ok();
    }

    /**
     * Propagates the deletion of a message received from another domain.
     * Delegates to {@link #deletePropagatedMessage(String, Long, String)} with
     * {@code null} deduplication metadata.
     *
     * @param mid message id to delete
     * @return OK on success, or an error result
     */
    @Override
    public Result<Void> deletePropagatedMessage(String mid) {
        return deletePropagatedMessage(mid, null, null);
    }

    /**
     * Propagates the deletion of a message received from another domain with
     * optional SID-based deduplication. When {@code sid} and
     * {@code sourceDomain} are non-null the call is a no-op if the same SID from
     * that domain was already processed.
     *
     * @param mid          message id to delete
     * @param sid          Kafka offset of the originating delete operation, or
     *                     {@code null}
     * @param sourceDomain domain that issued the delete request, or {@code null}
     * @return OK on success, or an error result
     */
    public Result<Void> deletePropagatedMessage(String mid, Long sid, String sourceDomain) {
        if (sid != null && sourceDomain != null) {
            if (registerSidIfNew(sourceDomain, sid)) {
                Log.info("Discarding duplicate deletePropagatedMessage from " + sourceDomain + " sid=" + sid);
                return Result.ok();
            }
        }
        ReplicatedOperation op = ReplicatedOperation.deletePropagatedMessage(mid, sid, sourceDomain);
        long offset = publisher.publish(topic, op.toJson());
        if (offset < 0)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        if (!waitForVersion(offset))
            return Result.error(Result.ErrorCode.TIMEOUT);
        return Result.ok();
    }

    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        // Publish a SYNC marker so we wait until this replica has applied all
        // preceding operations before executing the search (linearizability).
        long syncOffset = publisher.publish(topic, ReplicatedOperation.sync().toJson());
        if (syncOffset >= 0 && !waitForVersion(syncOffset)) {
            return Result.error(Result.ErrorCode.TIMEOUT);
        }
        return localImpl.searchInbox(name, pwd, query);
    }

    /**
     * Records {@code sid} as processed for {@code sourceDomain}.
     *
     * @param sourceDomain sending domain
     * @param sid          SID (Kafka offset) to register
     * @return {@code true} if this SID was already seen (duplicate), {@code false}
     *         if it was not previously registered (new)
     */
    private boolean registerSidIfNew(String sourceDomain, long sid) {
        java.util.Set<Long> seen = processedSidsPerDomain.computeIfAbsent(
                sourceDomain, ignored -> ConcurrentHashMap.newKeySet());
        return !seen.add(sid);
    }
}
