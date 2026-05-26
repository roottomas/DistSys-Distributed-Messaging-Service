package sd2526.trab.api.server.java;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import sd2526.trab.api.server.replication.InterDomainContext;

/**
 * Java implementation of the Messages service.
 *
 * <p>
 * This class is responsible for:
 * <ul>
 * <li>posting local messages;</li>
 * <li>forwarding remote messages asynchronously;</li>
 * <li>removing messages from inboxes;</li>
 * <li>deleting sent messages and propagating that deletion to remote
 * domains;</li>
 * <li>creating failure notifications for UNKNOWN USER and TIMEOUT cases.</li>
 * </ul>
 *
 * <p>
 * Remote propagation is done asynchronously using one FIFO queue per remote
 * domain,
 * which preserves operation ordering and isolates faults between domains.
 */
public class JavaMessages implements Messages {
    /** Logger used to record service activity and failures. */
    private static final Logger Log = Logger.getLogger(JavaMessages.class.getName());

    /**
     * Maximum time to keep retrying remote message operations before giving up
     * and, in the case of send operations, generating TIMEOUT notifications.
     */
    private static final long DEFAULT_EXTRA_LONG_FAULT_MILLIS = 90_000;

    /**
     * Maximum time to keep retrying contact with the Users service before
     * failing authentication or local/remote user lookup.
     */
    private static final long USERS_CONTACT_TIMEOUT_MILLIS = 30_000;
    private static final long REMOTE_SYNC_DISPATCH_WINDOW_MILLIS = 20_000;
    private static final long USER_NOT_FOUND_WARMUP_MILLIS = 2_000;

    /** Delay between retry attempts when communication faults occur. */
    private static final long RETRY_DELAY_MILLIS = 1_000;

    /** Persistence layer used to store and retrieve messages. */
    private final Hibernate hibernate;

    /** Supplier that provides the local domain of this server. */
    private final Supplier<String> localDomainSupplier;

    /** Client used to contact the local Users service. */
    private final UsersClient localUsersClient;

    /** Factory used to get Messages clients for specific remote domains. */
    private final RemoteMessagesClientFactory messagesClientFactory;

    /** Configured maximum duration for retrying remote message propagation. */
    private final long extraLongFaultMillis;

    /**
     * One single-thread executor per remote domain. Each executor owns an
     * internal FIFO task queue, which preserves operation ordering and isolates
     * faults between domains.
     */
    private final Map<String, ExecutorService> remoteExecutors;

    /**
     * Per-message locks used only to serialize local mutations on the same message.
     */
    private final Map<String, Object> messageLocks;

    /**
     * Stores original recipients by message id for response rendering.
     */
    private final Map<String, Set<String>> originalDestinations;

    /**
     * Replication sequence number of the operation currently being applied (set by
     * the replication layer).
     */
    private volatile long currentReplicationSid;

    /**
     * Represents an operation that must be propagated asynchronously to a remote
     * domain.
     */
    private static class RemoteOperation {
        /** Supported remote operation types. */
        enum Type {
            FORWARD, DELETE_PROPAGATED
        }

        /** Operation type. */
        final Type type;

        /** Target remote domain. */
        final String domain;

        /** Identifier of the message associated with the operation. */
        final String messageId;

        /** Message to forward, when the operation type is FORWARD. */
        final Message message;

        /** Original sender, used to create failure notifications on timeout. */
        final User senderUser;

        /**
         * Sender address in "name@domain" form; used when senderUser is null (phase-2
         * apply).
         */
        final String senderAddress;

        /**
         * Destinations that belong to the target domain.
         * Used to know which users should receive timeout notifications.
         */
        final Set<String> requestedInDomain;

        final long queuedAt;

        final long sid;

        /**
         * Creates a new remote operation descriptor (used by the non-replicated path
         * where
         * the full User object is available).
         */
        RemoteOperation(Type type, String domain, String messageId, Message message,
                User senderUser, Set<String> requestedInDomain, long sid) {
            this.type = type;
            this.domain = domain;
            this.messageId = messageId;
            this.message = message;
            this.senderUser = senderUser;
            this.senderAddress = senderUser != null
                    ? senderUser.getName() + "@" + senderUser.getDomain()
                    : null;
            this.requestedInDomain = requestedInDomain == null ? Set.of() : new HashSet<>(requestedInDomain);
            this.queuedAt = System.currentTimeMillis();
            this.sid = sid;
        }

        /**
         * Creates a new remote operation descriptor using a plain sender address string
         * (used by the replicated apply path where only the address is available).
         */
        RemoteOperation(Type type, String domain, String messageId, Message message,
                String senderAddress, Set<String> requestedInDomain, long sid) {
            this.type = type;
            this.domain = domain;
            this.messageId = messageId;
            this.message = message;
            this.senderUser = null;
            this.senderAddress = senderAddress;
            this.requestedInDomain = requestedInDomain == null ? Set.of() : new HashSet<>(requestedInDomain);
            this.queuedAt = System.currentTimeMillis();
            this.sid = sid;
        }
    }

    /**
     * Result of phase-1 resolution for a postMessage operation.
     * Carries all data needed to apply the operation in phase 2 without calling the
     * Users service.
     */
    public static class PostResolution {
        public final String mid;
        public final Set<String> persistedDestinations;
        public final Map<String, Set<String>> remoteForwardsByDomain;
        public final List<String> unknownDests;
        public final String senderAddress;

        public PostResolution(String mid, Set<String> persistedDestinations,
                Map<String, Set<String>> remoteForwardsByDomain,
                List<String> unknownDests, String senderAddress) {
            this.mid = mid;
            this.persistedDestinations = persistedDestinations;
            this.remoteForwardsByDomain = remoteForwardsByDomain;
            this.unknownDests = unknownDests;
            this.senderAddress = senderAddress;
        }
    }

    /**
     * Result of phase-1 resolution for a forwardMessage operation.
     * Carries resolved local destinations so phase 2 can persist without
     * re-querying Users.
     */
    public static class ForwardResolution {
        public final Set<String> validLocalDests;
        public final List<String> unknownLocalDests;

        public ForwardResolution(Set<String> validLocalDests, List<String> unknownLocalDests) {
            this.validLocalDests = validLocalDests;
            this.unknownLocalDests = unknownLocalDests;
        }
    }

    /**
     * Creates a service instance with default settings and default dependencies.
     */
    public JavaMessages() {
        this(Hibernate.getMessagesInstance(), () -> null, null, null, DEFAULT_EXTRA_LONG_FAULT_MILLIS);
    }

    /**
     * Creates a service instance with the provided dependencies and the default
     * extra-long-fault retry window.
     *
     * @param hibernate             persistence helper
     * @param localDomainSupplier   supplier for the local domain
     * @param localUsersClient      client for the local Users service
     * @param messagesClientFactory factory for remote Messages clients
     */
    public JavaMessages(Hibernate hibernate,
            Supplier<String> localDomainSupplier,
            UsersClient localUsersClient,
            RemoteMessagesClientFactory messagesClientFactory) {
        this(hibernate, localDomainSupplier, localUsersClient, messagesClientFactory,
                DEFAULT_EXTRA_LONG_FAULT_MILLIS);
    }

    /**
     * Creates a fully configured service instance.
     *
     * @param hibernate             persistence helper
     * @param localDomainSupplier   supplier for the local domain
     * @param localUsersClient      client for the local Users service
     * @param messagesClientFactory factory for remote Messages clients
     * @param extraLongFaultMillis  maximum retry window for remote propagation
     */
    public JavaMessages(Hibernate hibernate,
            Supplier<String> localDomainSupplier,
            UsersClient localUsersClient,
            RemoteMessagesClientFactory messagesClientFactory,
            long extraLongFaultMillis) {
        this.hibernate = hibernate;
        this.localDomainSupplier = localDomainSupplier;
        this.localUsersClient = localUsersClient;
        this.messagesClientFactory = messagesClientFactory;
        this.extraLongFaultMillis = extraLongFaultMillis;
        this.remoteExecutors = new ConcurrentHashMap<>();
        this.messageLocks = new ConcurrentHashMap<>();
        this.originalDestinations = new ConcurrentHashMap<>();
        this.currentReplicationSid = -1;
    }

    /**
     * Sets the Kafka offset of the replication operation that is currently being
     * applied. Called by
     * {@link sd2526.trab.api.server.replication.ReplicatedMessages}
     * before delegating a phase-2 apply call so that remote-forward operations
     * stamped with this sid can be deduplicated at the receiving replica.
     *
     * @param sid Kafka offset of the operation being applied, or {@code -1} to
     *            clear the value after the apply completes
     */
    public void setCurrentReplicationSid(long sid) {
        this.currentReplicationSid = sid;
    }

    /**
     * Returns the lock object associated with a given message id.
     *
     * @param mid message id
     * @return a stable lock object for that message id
     */
    private Object getMessageLock(String mid) {
        return messageLocks.computeIfAbsent(mid, k -> new Object());
    }

    /**
     * Retrieves the local domain name.
     *
     * @return the local domain name, or null if retrieval fails
     */
    private String localDomain() {
        try {
            return localDomainSupplier.get();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Extracts the username from a plain name, an address, or a formatted sender
     * string.
     *
     * @param nameOrAddress raw user name, address, or formatted sender
     * @return normalized username, or null if input is null
     */
    private String normalizeUserName(String nameOrAddress) {
        if (nameOrAddress == null)
            return null;

        String s = nameOrAddress.trim();
        int lt = s.indexOf('<');
        int at = s.indexOf('@');
        int gt = s.indexOf('>');

        if (lt >= 0 && at > lt && gt > at)
            return s.substring(lt + 1, at).trim();
        if (at > 0)
            return s.substring(0, at).trim();
        return s;
    }

    /**
     * Returns the canonical address for a user.
     *
     * @param user user
     * @return address in the form name@domain
     */
    private String userAddress(User user) {
        return user.getName() + "@" + user.getDomain();
    }

    /**
     * Normalizes a destination string into a canonical address.
     *
     * <p>
     * The input may be:
     * <ul>
     * <li>a plain username</li>
     * <li>a user@domain address</li>
     * <li>a formatted sender string containing &lt;user@domain&gt;</li>
     * </ul>
     *
     * @param nameOrAddress raw destination
     * @param defaultDomain domain to use when no explicit domain is present
     * @return normalized address, or null if input is null
     */
    private String normalizeAddress(String nameOrAddress, String defaultDomain) {
        if (nameOrAddress == null)
            return null;

        String s = nameOrAddress.trim();
        int lt = s.indexOf('<');
        int at = s.indexOf('@');
        int gt = s.indexOf('>');

        if (lt >= 0 && at > lt && gt > at)
            return s.substring(lt + 1, gt).trim();
        if (s.contains("@"))
            return s;
        return s + "@" + defaultDomain;
    }

    /**
     * Returns the username component of an address.
     *
     * @param address address in the form name@domain
     * @return the name part, or the original string if no domain exists
     */
    private String addressName(String address) {
        int at = address.indexOf('@');
        return at >= 0 ? address.substring(0, at) : address;
    }

    /**
     * Returns the domain component of an address.
     *
     * @param address address in the form name@domain
     * @return the domain part, or null if missing
     */
    private String addressDomain(String address) {
        int at = address.indexOf('@');
        return at >= 0 ? address.substring(at + 1) : null;
    }

    /**
     * Formats the sender field as "Display Name &lt;name@domain&gt;".
     *
     * @param user sender user
     * @return formatted sender string
     */
    private String formatSender(User user) {
        return user.getDisplayName() + " <" + userAddress(user) + ">";
    }

    /**
     * Checks whether a result corresponds to a retriable communication failure.
     *
     * @param result result to inspect
     * @return true if the result is INTERNAL_ERROR or TIMEOUT
     */
    private boolean isRetriableError(Result<?> result) {
        return result != null && !result.isOK()
                && (result.error() == Result.ErrorCode.INTERNAL_ERROR || result.error() == Result.ErrorCode.TIMEOUT);
    }

    /**
     * Sleeps for the configured retry delay.
     */
    private void sleepBeforeRetry() {
        try {
            Thread.sleep(RETRY_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Executes an operation and retries it while it fails with a retriable error.
     *
     * <p>
     * Retries stop when the timeout deadline is reached or the current thread is
     * interrupted.
     *
     * @param operation operation to execute
     * @param <T>       result type
     * @return successful result, first non-retriable failure, or
     *         TIMEOUT/INTERNAL_ERROR if retries cannot continue
     */
    private <T> Result<T> retryUsersOperation(Supplier<Result<T>> operation) {
        long deadline = System.currentTimeMillis() + USERS_CONTACT_TIMEOUT_MILLIS;

        while (true) {
            Result<T> result = operation.get();
            if (!isRetriableError(result))
                return result;

            if (System.currentTimeMillis() >= deadline) {
                return Result.error(Result.ErrorCode.TIMEOUT);
            }
            sleepBeforeRetry();
            if (Thread.currentThread().isInterrupted()) {
                return Result.error(Result.ErrorCode.INTERNAL_ERROR);
            }
        }
    }

    /**
     * Authenticates a user by invoking the local Users service, retrying on
     * transient failures.
     *
     * @param name user name
     * @param pwd  user password
     * @return authenticated user if successful, or an error result
     */
    private Result<User> authenticateUser(String name, String pwd) {
        if (name == null || pwd == null) {
            Log.info("Name or password is null.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }
        if (localUsersClient == null)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);

        return retryUsersOperation(() -> localUsersClient.getUser(name, pwd));
    }

    /**
     * Retrieves a user from the local domain using the internal Users operation.
     *
     * @param targetName username to find
     * @return matching local user, or an error result
     */
    private Result<User> findUserInLocalDomain(String targetName) {
        try {
            if (localUsersClient == null)
                return Result.error(Result.ErrorCode.INTERNAL_ERROR);
            return retryUsersOperation(() -> localUsersClient.getInternalUser(targetName));
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    private Result<User> findUserInLocalDomainWithWarmup(String targetName) {
        long deadline = System.currentTimeMillis() + USER_NOT_FOUND_WARMUP_MILLIS;

        while (true) {
            Result<User> result = findUserInLocalDomain(targetName);
            if (result.isOK() || result.error() != Result.ErrorCode.NOT_FOUND)
                return result;

            if (System.currentTimeMillis() >= deadline)
                return result;

            sleepBeforeRetry();
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                return Result.error(Result.ErrorCode.INTERNAL_ERROR);
            }
        }
    }

    /**
     * Produces a canonical string representation of a message.
     *
     * <p>
     * Destinations are sorted so that logically equivalent messages generate
     * the same fingerprint independently of destination order.
     *
     * @param msg message
     * @return canonical fingerprint string
     */
    private String canonicalMessageFingerprint(Message msg) {
        Set<String> sortedDests = new TreeSet<>(msg.getDestination());
        return msg.getSender() + "|" + sortedDests + "|" + msg.getSubject() + "|" + msg.getContents();
    }

    /**
     * Generates a deterministic message id from a canonical fingerprint.
     *
     * @param msg message
     * @return deterministic UUID-based message id
     */
    private String deterministicMessageId(Message msg) {
        return UUID.nameUUIDFromBytes(canonicalMessageFingerprint(msg).getBytes()).toString();
    }

    /**
     * Creates a detached copy of a message with the provided id and destinations.
     *
     * @param id           id to assign
     * @param source       source message
     * @param destinations destination set for the copy
     * @return copied message
     */
    private Message copyMessage(String id, Message source, Set<String> destinations) {
        Message copy = new Message();
        copy.setId(id);
        copy.setSender(source.getSender());
        copy.setDestination(new HashSet<>(destinations));
        copy.setSubject(source.getSubject());
        copy.setContents(source.getContents());
        copy.setCreationTime(source.getCreationTime());
        return copy;
    }

    /**
     * Persists a local copy of a message if it has destinations and is not yet
     * stored.
     *
     * @param mid             message id
     * @param base            source message
     * @param allDestinations destinations to associate with the persisted message
     * @return OK on success or if already present, INTERNAL_ERROR on unexpected
     *         failure
     */
    private Result<Void> persistLocalMessage(String mid, Message base, Set<String> allDestinations) {
        if (allDestinations == null || allDestinations.isEmpty())
            return Result.ok();

        try {
            synchronized (getMessageLock(mid)) {
                originalDestinations.merge(mid, new HashSet<>(allDestinations), (current, incoming) -> {
                    Set<String> merged = new HashSet<>(current);
                    merged.addAll(incoming);
                    return merged;
                });

                Message existing = hibernate.get(Message.class, mid);
                if (existing != null) {
                    return Result.ok();
                }

                Message local = copyMessage(mid, base, allDestinations);
                hibernate.persist(local);
                return Result.ok();
            }
        } catch (Exception e) {
            try {
                synchronized (getMessageLock(mid)) {
                    Message existing = hibernate.get(Message.class, mid);
                    if (existing != null)
                        return Result.ok();
                }
            } catch (Exception ignored) {
                // Best effort re-check to make persistence idempotent under races.
            }

            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Creates and persists a failure notification informing the sender that
     * a destination is unknown.
     */
    private void createUnknownUserNotification(String senderAddress, String originalMid,
            String unknownDestination, Message original) {
        try {
            Message failure = new Message();
            failure.setId(originalMid + "." + unknownDestination);
            failure.setSender(original.getSender());
            failure.setDestination(Set.of(senderAddress));
            failure.setSubject("FAILED TO SEND " + originalMid + " TO " + unknownDestination + ": UNKNOWN USER");
            failure.setContents(original.getContents());
            failure.setCreationTime(original.getCreationTime());

            Message existing = hibernate.get(Message.class, failure.getId());
            if (existing == null)
                hibernate.persist(failure);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createUnknownUserNotification(User senderUser, String originalMid,
            String unknownDestination, Message original) {
        createUnknownUserNotification(userAddress(senderUser), originalMid, unknownDestination, original);
    }

    /**
     * Creates and persists a failure notification informing the sender that
     * delivery timed out.
     */
    private void createTimeoutNotification(String senderAddress, String originalMid,
            String failedDestination, Message original) {
        try {
            Message failure = new Message();
            failure.setId(originalMid + "." + failedDestination);
            failure.setSender(original.getSender());
            failure.setDestination(Set.of(senderAddress));
            failure.setSubject("FAILED TO SEND " + originalMid + " TO " + failedDestination + ": TIMEOUT");
            failure.setContents(original.getContents());
            failure.setCreationTime(original.getCreationTime());

            Message existing = hibernate.get(Message.class, failure.getId());
            if (existing == null)
                hibernate.persist(failure);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieves a message from the authenticated user's inbox, ensuring ownership.
     *
     * @param authUser authenticated user
     * @param mid      message id
     * @return message if it exists and belongs to the user, otherwise an error
     *         result
     */
    private Result<Message> getAuthorizedInboxMessage(User authUser, String mid) {
        if (authUser == null) {
            Log.info("Authenticated user is null.");
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }

        if (mid == null) {
            Log.info("Message id is null.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        try {
            synchronized (getMessageLock(mid)) {
                Message msg = hibernate.get(Message.class, mid);
                if (msg == null) {
                    Log.info("Message not found.");
                    return Result.error(Result.ErrorCode.NOT_FOUND);
                }

                String addr = userAddress(authUser);
                Set<String> destinations = msg.getDestination() == null
                        ? Set.of()
                        : new HashSet<>(msg.getDestination());
                if (!destinations.contains(addr)) {
                    Log.info("Message does not belong to this user's inbox.");
                    return Result.error(Result.ErrorCode.NOT_FOUND);
                }

                Set<String> original = originalDestinations.get(mid);
                Set<String> visibleDestinations = original == null || original.isEmpty()
                        ? destinations
                        : new HashSet<>(original);

                return Result.ok(copyMessage(mid, msg, visibleDestinations));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Returns the single-thread executor associated with a remote domain,
     * creating it if necessary.
     *
     * <p>
     * Each executor processes submitted remote operations sequentially for its
     * domain, which preserves ordering while keeping domains isolated from each
     * other.
     *
     * @param domain remote domain
     * @return single-thread executor for that domain
     */
    private ExecutorService getRemoteExecutor(String domain) {
        ExecutorService existing = remoteExecutors.get(domain);
        if (existing != null)
            return existing;

        ExecutorService created = Executors.newSingleThreadExecutor(runnable -> {
            Thread worker = new Thread(runnable, "messages-remote-worker-" + domain);
            worker.setDaemon(true);
            return worker;
        });

        ExecutorService previous = remoteExecutors.putIfAbsent(domain, created);
        if (previous != null) {
            created.shutdown();
            return previous;
        }
        return created;
    }

    /**
     * Submits a remote operation to the single-thread executor of the target
     * domain.
     *
     * @param operation operation to execute asynchronously
     */
    private void submitRemoteOperation(RemoteOperation operation) {
        ExecutorService executor = getRemoteExecutor(operation.domain);
        executor.submit(() -> processRemoteOperationUntilDone(operation));
    }

    /**
     * Tries a remote operation once immediately, then falls back to async retries.
     */
    private void dispatchRemoteOperation(RemoteOperation operation) {
        long deadline = System.currentTimeMillis() + REMOTE_SYNC_DISPATCH_WINDOW_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (processRemoteOperation(operation))
                    return;
            } catch (Exception e) {
                Log.warning("Immediate remote operation failed for domain "
                        + operation.domain + ": " + e.getMessage());
            }

            sleepBeforeRetry();
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        submitRemoteOperation(operation);
    }

    /**
     * Processes a remote operation until it either succeeds or expires.
     *
     * @param operation operation to process
     */
    private void processRemoteOperationUntilDone(RemoteOperation operation) {
        boolean done = false;

        while (!done) {
            try {
                done = processRemoteOperation(operation);
                if (!done) {
                    sleepBeforeRetry();
                    if (Thread.currentThread().isInterrupted())
                        return;
                }
            } catch (Exception e) {
                Log.warning("Unexpected error processing remote operation for domain "
                        + operation.domain + ": " + e.getMessage());
                sleepBeforeRetry();
                if (Thread.currentThread().isInterrupted())
                    return;
            }
        }
    }

    /**
     * Processes a single remote operation.
     *
     * @param operation operation to process
     * @return true if completed, false if it should be retried later
     */
    private boolean processRemoteOperation(RemoteOperation operation) {
        if (System.currentTimeMillis() - operation.queuedAt >= extraLongFaultMillis)
            return expireRemoteOperation(operation);

        return switch (operation.type) {
            case FORWARD -> processForwardOperation(operation);
            case DELETE_PROPAGATED -> processDeleteOperation(operation);
        };
    }

    /**
     * Expires a remote operation after the retry window is exhausted.
     *
     * <p>
     * For forwarding operations, one TIMEOUT notification is generated per
     * destination.
     *
     * @param operation operation to expire
     * @return true because expiration finalizes the operation
     */
    private boolean expireRemoteOperation(RemoteOperation operation) {
        if (operation.type == RemoteOperation.Type.FORWARD && operation.senderAddress != null) {
            for (String dest : operation.requestedInDomain)
                createTimeoutNotification(operation.senderAddress, operation.messageId, dest, operation.message);
        }
        return true;
    }

    /**
     * Processes a remote forward operation.
     *
     * @param operation operation to process
     * @return true if forwarding succeeded, false if it should be retried
     */
    private boolean processForwardOperation(RemoteOperation operation) {
        try {
            InterDomainContext.sid.set(operation.sid);
            InterDomainContext.sourceDomain.set(localDomain());
            try {
                Result<List<String>> forwardResult = messagesClientFactory.get(operation.domain)
                        .forwardMessage(operation.message);
                if (!forwardResult.isOK())
                    return false;

                for (String unknown : forwardResult.value())
                    createUnknownUserNotification(operation.senderAddress, operation.messageId, unknown,
                            operation.message);

                return true;
            } finally {
                InterDomainContext.sid.remove();
                InterDomainContext.sourceDomain.remove();
            }
        } catch (Exception e) {
            Log.warning("Failed forwarding message " + operation.messageId + " to domain "
                    + operation.domain + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Processes a remote propagated delete operation.
     *
     * @param operation operation to process
     * @return true if deletion succeeded, false if it should be retried
     */
    private boolean processDeleteOperation(RemoteOperation operation) {
        try {
            InterDomainContext.sid.set(operation.sid);
            InterDomainContext.sourceDomain.set(localDomain());
            try {
                Result<Void> deleteResult = messagesClientFactory.get(operation.domain)
                        .deletePropagatedMessage(operation.messageId);
                return deleteResult.isOK();
            } finally {
                InterDomainContext.sid.remove();
                InterDomainContext.sourceDomain.remove();
            }
        } catch (Exception e) {
            Log.warning("Failed deleting propagated message " + operation.messageId + " in domain "
                    + operation.domain + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Phase 1 for postMessage: authenticates the sender, resolves all destinations,
     * and returns the pre-resolved data needed for phase 2.
     * No side effects (no persistence, no remote calls).
     */
    public Result<PostResolution> resolvePost(String pwd, Message msg) {
        if (pwd == null || msg == null || msg.getSender() == null || msg.getDestination() == null
                || msg.getDestination().isEmpty() || msg.getSubject() == null || msg.getContents() == null)
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        String currentDomain = localDomain();
        if (currentDomain == null)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);

        String senderName = normalizeUserName(msg.getSender());
        Result<User> auth = authenticateUser(senderName, pwd);
        if (!auth.isOK())
            return Result.error(auth);

        User senderUser = auth.value();

        Result<Map<String, Set<String>>> groupedResult = prepareAndGroupDestinations(msg, senderUser, currentDomain);
        if (!groupedResult.isOK())
            return Result.error(groupedResult);

        String mid = msg.getId();
        if (mid == null) {
            mid = deterministicMessageId(msg);
            msg.setId(mid);
        }

        Map<String, Set<String>> byDomain = groupedResult.value();

        // Resolve local destinations
        Set<String> validLocal = new HashSet<>();
        List<String> unknownDests = new ArrayList<>();
        for (String dest : byDomain.getOrDefault(currentDomain, Set.of())) {
            String destName = addressName(dest);
            Result<User> destUser = findUserInLocalDomainWithWarmup(destName);
            if (!destUser.isOK()) {
                if (destUser.error() == Result.ErrorCode.NOT_FOUND)
                    unknownDests.add(dest);
                else
                    return Result.error(destUser);
            } else {
                validLocal.add(userAddress(destUser.value()));
            }
        }

        // Build remote-forwards map and persisted-destinations set
        Map<String, Set<String>> remoteForwards = new HashMap<>();
        Set<String> persistedDests = new HashSet<>(validLocal);
        for (Map.Entry<String, Set<String>> entry : byDomain.entrySet()) {
            if (!currentDomain.equals(entry.getKey())) {
                remoteForwards.put(entry.getKey(), entry.getValue());
                persistedDests.addAll(entry.getValue());
            }
        }

        return Result
                .ok(new PostResolution(mid, persistedDests, remoteForwards, unknownDests, userAddress(senderUser)));
    }

    /**
     * Phase 2 for postMessage: applies a pre-resolved post with no Users-service
     * calls.
     * Persists the message, creates failure notifications, and enqueues remote
     * forwards.
     */
    public Result<String> applyPost(Message msg, String mid,
            Set<String> persistedDests,
            Map<String, Set<String>> remoteForwards,
            List<String> unknownDests,
            String senderAddress) {
        msg.setId(mid);

        // Failure notifications for unknown users
        for (String unknown : unknownDests)
            createUnknownUserNotification(senderAddress, mid, unknown, msg);

        // Persist locally
        if (!persistedDests.isEmpty()) {
            Result<Void> persisted = persistLocalMessage(mid, msg, persistedDests);
            if (!persisted.isOK())
                return Result.error(persisted);
        }

        // Enqueue remote forwards (each replica does this; sid dedup at remote domain
        // handles duplication)
        for (Map.Entry<String, Set<String>> entry : remoteForwards.entrySet()) {
            Message forward = copyMessage(mid, msg, msg.getDestination());
            RemoteOperation remOp = new RemoteOperation(
                    RemoteOperation.Type.FORWARD,
                    entry.getKey(), mid, forward,
                    senderAddress, entry.getValue(), currentReplicationSid);
            dispatchRemoteOperation(remOp);
        }

        return Result.ok(mid);
    }

    /**
     * Phase 1 for forwardMessage: resolves local destinations without persisting.
     * Returns which local users exist and which are unknown.
     */
    public Result<ForwardResolution> resolveForward(Message msg) {
        if (msg == null || msg.getId() == null || msg.getSender() == null
                || msg.getDestination() == null || msg.getDestination().isEmpty()
                || msg.getSubject() == null || msg.getContents() == null)
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        String currentDomain = localDomain();
        if (currentDomain == null)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);

        Set<String> validLocalDests = new HashSet<>();
        List<String> unknownLocalDests = new ArrayList<>();

        for (String dest : msg.getDestination()) {
            String normalized = normalizeAddress(dest, currentDomain);
            if (!currentDomain.equals(addressDomain(normalized)))
                continue;

            String destName = addressName(normalized);
            Result<User> destUser = findUserInLocalDomainWithWarmup(destName);
            if (!destUser.isOK()) {
                if (destUser.error() == Result.ErrorCode.NOT_FOUND)
                    unknownLocalDests.add(normalized);
                else
                    return Result.error(destUser);
            } else {
                validLocalDests.add(userAddress(destUser.value()));
            }
        }

        return Result.ok(new ForwardResolution(validLocalDests, unknownLocalDests));
    }

    /**
     * Phase 2 for forwardMessage: persists the message for pre-resolved valid
     * destinations
     * without calling the Users service.
     */
    public Result<List<String>> applyForward(Message msg, Set<String> validLocalDests,
            List<String> unknownDests) {
        if (!validLocalDests.isEmpty()) {
            Result<Void> persisted = persistLocalMessage(msg.getId(), msg, new HashSet<>(msg.getDestination()));
            if (!persisted.isOK())
                return Result.error(persisted);
        }
        return Result.ok(unknownDests != null ? unknownDests : List.of());
    }

    /**
     * Posts a message on behalf of an authenticated sender.
     *
     * <p>
     * This method:
     * <ol>
     * <li>authenticates the sender;</li>
     * <li>normalizes sender and destinations;</li>
     * <li>validates local recipients;</li>
     * <li>enqueues remote forwarding operations;</li>
     * <li>persists the message locally if at least one valid destination
     * exists.</li>
     * </ol>
     *
     * @param pwd sender password
     * @param msg message to post
     * @return generated or existing message id on success, or an error result
     */
    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Log.info("postMessage : pwd = " + pwd + "; msg = " + msg);

        if (pwd == null || msg == null || msg.getSender() == null || msg.getDestination() == null
                || msg.getDestination().isEmpty() || msg.getSubject() == null || msg.getContents() == null) {
            Log.info("Invalid message.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        String currentDomain = localDomain();
        if (currentDomain == null) {
            Log.info("Could not determine local domain.");
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }

        String senderName = normalizeUserName(msg.getSender());
        Result<User> auth = authenticateUser(senderName, pwd);
        if (!auth.isOK())
            return Result.error(auth);

        User senderUser = auth.value();

        try {
            Result<Map<String, Set<String>>> groupedResult = prepareAndGroupDestinations(msg, senderUser,
                    currentDomain);
            if (!groupedResult.isOK())
                return Result.error(groupedResult);

            String originalMid = msg.getId();
            if (originalMid == null) {
                originalMid = deterministicMessageId(msg);
                msg.setId(originalMid);
            }

            Map<String, Set<String>> byDomain = groupedResult.value();

            Result<Set<String>> localResult = handleLocalDestinations(
                    senderUser,
                    originalMid,
                    msg,
                    byDomain.getOrDefault(currentDomain, Set.of()));

            if (!localResult.isOK())
                return Result.error(localResult);

            Result<Boolean> remoteResult = handleRemoteDestinations(
                    senderUser,
                    originalMid,
                    msg,
                    currentDomain,
                    byDomain);

            if (!remoteResult.isOK())
                return Result.error(remoteResult);

            Set<String> persistedDestinations = new HashSet<>(localResult.value());
            for (Map.Entry<String, Set<String>> entry : byDomain.entrySet()) {
                if (!currentDomain.equals(entry.getKey()))
                    persistedDestinations.addAll(entry.getValue());
            }

            if (!persistedDestinations.isEmpty()) {
                Result<Void> persisted = persistLocalMessage(originalMid, msg, persistedDestinations);
                if (!persisted.isOK())
                    return Result.error(persisted);
            }

            return Result.ok(originalMid);

        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Normalizes sender and destinations and groups destinations by domain.
     *
     * @param msg           message being posted
     * @param senderUser    authenticated sender
     * @param currentDomain local domain used as default when a destination omits
     *                      its domain
     * @return map from domain to normalized destination set, or an error result if
     *         a destination is invalid
     */
    private Result<Map<String, Set<String>>> prepareAndGroupDestinations(Message msg, User senderUser,
            String currentDomain) {
        msg.setSender(formatSender(senderUser));

        Set<String> normalizedDestinations = new HashSet<>();
        for (String d : msg.getDestination())
            normalizedDestinations.add(normalizeAddress(d, currentDomain));
        msg.setDestination(normalizedDestinations);

        Map<String, Set<String>> byDomain = new HashMap<>();
        for (String dest : normalizedDestinations) {
            String domain = addressDomain(dest);
            if (domain == null) {
                Log.info("Invalid destination: " + dest);
                return Result.error(Result.ErrorCode.BAD_REQUEST);
            }
            byDomain.computeIfAbsent(domain, k -> new HashSet<>()).add(dest);
        }

        return Result.ok(byDomain);
    }

    /**
     * Validates the local-domain destinations of a message.
     *
     * <p>
     * Unknown users generate failure notifications. Any non-NOT_FOUND failure
     * during lookup is returned immediately.
     *
     * @param senderUser     authenticated sender
     * @param originalMid    original message id
     * @param msg            original message
     * @param localRequested local destinations
     * @return the valid local destination addresses, or an error result
     */
    private Result<Set<String>> handleLocalDestinations(User senderUser, String originalMid, Message msg,
            Set<String> localRequested) {
        Set<String> validLocal = new HashSet<>();
        Set<String> localUnknown = new HashSet<>();

        for (String dest : localRequested) {
            String destName = addressName(dest);
            Result<User> destUser = findUserInLocalDomainWithWarmup(destName);

            if (!destUser.isOK()) {
                if (destUser.error() == Result.ErrorCode.NOT_FOUND)
                    localUnknown.add(dest);
                else
                    return Result.error(destUser);
            } else {
                validLocal.add(userAddress(destUser.value()));
            }
        }

        for (String unknown : localUnknown)
            createUnknownUserNotification(senderUser, originalMid, unknown, msg);

        return Result.ok(validLocal);
    }

    /**
     * Enqueues forwarding operations for all non-local destination domains.
     *
     * @param senderUser    authenticated sender
     * @param originalMid   original message id
     * @param msg           original message
     * @param currentDomain local domain
     * @param byDomain      destinations grouped by domain
     * @return true if at least one remote forwarding operation was enqueued
     */
    private Result<Boolean> handleRemoteDestinations(User senderUser, String originalMid, Message msg,
            String currentDomain, Map<String, Set<String>> byDomain) {
        boolean hasAnyRemoteDestination = false;

        for (Map.Entry<String, Set<String>> entry : byDomain.entrySet()) {
            String domain = entry.getKey();

            if (currentDomain.equals(domain))
                continue;

            Set<String> requestedInDomain = entry.getValue();
            Message forward = copyMessage(originalMid, msg, msg.getDestination());
            RemoteOperation op = new RemoteOperation(
                    RemoteOperation.Type.FORWARD,
                    domain,
                    originalMid,
                    forward,
                    senderUser,
                    requestedInDomain,
                    currentReplicationSid);

            dispatchRemoteOperation(op);
            hasAnyRemoteDestination = true;
        }

        return Result.ok(hasAnyRemoteDestination);
    }

    /**
     * Processes a message that was already forwarded by another domain into
     * this domain.
     *
     * <p>
     * This method is the receiving side of the
     * inter-domain forwarding protocol. Internally, what it really does is
     * validate the local recipients of the forwarded message, store the local
     * deliveries and report which local destinations do not exist.
     *
     * @param msg message received from a remote domain for local delivery
     * @return list of unknown local destinations, or an error result
     */
    @Override
    public Result<List<String>> forwardMessage(Message msg) {
        Log.info("forwardMessage : msg = " + msg);

        if (msg == null || msg.getId() == null || msg.getSender() == null || msg.getDestination() == null
                || msg.getDestination().isEmpty() || msg.getSubject() == null || msg.getContents() == null) {
            Log.info("Invalid forwarded message.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        String currentDomain = localDomain();
        if (currentDomain == null) {
            Log.info("Could not determine local domain.");
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }

        try {
            Set<String> validLocal = new HashSet<>();
            List<String> unknownLocal = new ArrayList<>();

            for (String dest : msg.getDestination()) {
                String normalized = normalizeAddress(dest, currentDomain);
                String domain = addressDomain(normalized);
                if (!currentDomain.equals(domain))
                    continue;

                String destName = addressName(normalized);
                Result<User> destUser = findUserInLocalDomainWithWarmup(destName);
                if (!destUser.isOK()) {
                    if (destUser.error() == Result.ErrorCode.NOT_FOUND)
                        unknownLocal.add(normalized);
                    else
                        return Result.error(destUser);
                } else {
                    validLocal.add(userAddress(destUser.value()));
                }
            }

            if (!validLocal.isEmpty()) {
                Result<Void> persisted = persistLocalMessage(msg.getId(), msg, new HashSet<>(msg.getDestination()));
                if (!persisted.isOK())
                    return Result.error(persisted);
            }

            return Result.ok(unknownLocal);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Retrieves a specific message from the authenticated user's inbox.
     *
     * @param name user name
     * @param mid  message id
     * @param pwd  user password
     * @return the inbox message if authorized, or an error result
     */
    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        Log.info("getMessage : name = " + name + "; mid = " + mid + "; pwd = " + pwd);
        return getAuthorizedInboxMessage(name, pwd, mid);
    }

    /**
     * Returns all message ids currently present in the authenticated user's inbox.
     *
     * @param name user name
     * @param pwd  user password
     * @return list of inbox message ids, or an error result
     */
    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        return getMessages(name, pwd, "");
    }

    /**
     * Phase 1 for {@link #removeInboxMessage}: authenticates the requesting user
     * and returns their canonical address so that phase 2 can apply the removal
     * without calling the Users service again.
     *
     * @param name user name
     * @param pwd  user password
     * @param mid  message id to remove
     * @return the authenticated user's canonical address on success, or an error
     *         result if authentication fails or {@code mid} is null
     */
    public Result<String> resolveRemoveInbox(String name, String pwd, String mid) {
        if (mid == null)
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        Result<User> auth = authenticateUser(name, pwd);
        if (!auth.isOK())
            return Result.error(auth);

        return Result.ok(userAddress(auth.value()));
    }

    /**
     * Phase 2 for {@link #removeInboxMessage}: removes {@code userAddress} from
     * the destination set of message {@code mid} without calling the Users
     * service. If no destinations remain the message is deleted.
     *
     * <p>
     * The operation is idempotent: if the message does not exist or the user
     * is no longer in its destination set the call succeeds silently.
     *
     * @param userAddress canonical address of the user whose inbox entry is
     *                    being removed
     * @param mid         message id
     * @return OK on success, BAD_REQUEST if either argument is null, or
     *         INTERNAL_ERROR on unexpected persistence failures
     */
    public Result<Void> applyRemoveInboxMessage(String userAddress, String mid) {
        if (userAddress == null || mid == null)
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        try {
            synchronized (getMessageLock(mid)) {
                Message msg = hibernate.get(Message.class, mid);
                if (msg == null)
                    return Result.ok();

                Set<String> destinations = msg.getDestination() == null
                        ? new HashSet<>()
                        : new HashSet<>(msg.getDestination());

                if (!destinations.contains(userAddress))
                    return Result.ok();

                destinations.remove(userAddress);
                if (destinations.isEmpty()) {
                    hibernate.delete(msg);
                    originalDestinations.remove(mid);
                } else {
                    msg.setDestination(destinations);
                    hibernate.update(msg);
                }
            }
            return Result.ok();
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Phase 1 for {@link #deleteMessage}: authenticates the requesting sender
     * and returns their canonical address so that phase 2 can apply the deletion
     * without calling the Users service again.
     *
     * @param name user name
     * @param pwd  user password
     * @param mid  message id to delete
     * @return the authenticated user's canonical address on success, or an error
     *         result if authentication fails or {@code mid} is null
     */
    public Result<String> resolveDeleteMessage(String name, String pwd, String mid) {
        if (mid == null)
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        Result<User> auth = authenticateUser(name, pwd);
        if (!auth.isOK())
            return Result.error(auth);

        return Result.ok(userAddress(auth.value()));
    }

    /**
     * Phase 2 for {@link #deleteMessage}: verifies sender ownership, deletes the
     * local copy of the message, and enqueues propagated-delete operations for
     * every remote domain that has a copy, without calling the Users service.
     *
     * @param userAddress canonical address of the user who sent the message
     * @param mid         message id
     * @return OK on success (including if the message was already absent),
     *         FORBIDDEN if the caller is not the sender, BAD_REQUEST if either
     *         argument is null, or INTERNAL_ERROR on unexpected failures
     */
    public Result<Void> applyDeleteMessage(String userAddress, String mid) {
        if (userAddress == null || mid == null)
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        String currentDomain = localDomain();
        if (currentDomain == null)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);

        Set<String> remoteDomains = new HashSet<>();
        try {
            synchronized (getMessageLock(mid)) {
                Message msg = hibernate.get(Message.class, mid);
                if (msg == null)
                    return Result.ok();

                String senderAddress = normalizeAddress(msg.getSender(), currentDomain);
                if (!userAddress.equals(senderAddress))
                    return Result.error(Result.ErrorCode.FORBIDDEN);

                Set<String> destinations = msg.getDestination() == null
                        ? Set.of()
                        : new HashSet<>(msg.getDestination());

                for (String dest : destinations) {
                    String domain = addressDomain(dest);
                    if (domain != null && !domain.equals(currentDomain))
                        remoteDomains.add(domain);
                }

                hibernate.delete(msg);
                originalDestinations.remove(mid);
            }

            for (String domain : remoteDomains) {
                RemoteOperation op = new RemoteOperation(
                        RemoteOperation.Type.DELETE_PROPAGATED,
                        domain,
                        mid,
                        null,
                        (String) null,
                        null,
                        currentReplicationSid);
                dispatchRemoteOperation(op);
            }

            return Result.ok();
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Removes a message from the authenticated user's inbox.
     *
     * <p>
     * If the user is the last recipient in this stored copy, the message is
     * deleted from storage. Otherwise, only the user's address is removed from
     * the destination set.
     *
     * @param name user name
     * @param mid  message id
     * @param pwd  user password
     * @return OK if removed, or an error result
     */
    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Log.info("removeFromUserInbox : name = " + name + "; mid = " + mid + "; pwd = " + pwd);

        Result<String> resolved = resolveRemoveInbox(name, pwd, mid);
        if (!resolved.isOK())
            return Result.error(resolved);

        String addr = resolved.value();

        try {
            synchronized (getMessageLock(mid)) {
                Message msg = hibernate.get(Message.class, mid);

                // Idempotent under concurrent interference:
                // if sender already deleted it, inbox removal should still succeed.
                if (msg == null) {
                    Log.info("Message already absent.");
                    return Result.ok();
                }

                Set<String> destinations = msg.getDestination() == null
                        ? new HashSet<>()
                        : new HashSet<>(msg.getDestination());

                // Also treat "already removed from this inbox" as success.
                if (!destinations.contains(addr)) {
                    Log.info("Message already not present in this user's inbox.");
                    return Result.ok();
                }

                destinations.remove(addr);

                if (destinations.isEmpty()) {
                    hibernate.delete(msg);
                    originalDestinations.remove(mid);
                } else {
                    msg.setDestination(destinations);
                    hibernate.update(msg);
                }
            }

            return Result.ok();
        } catch (Exception x) {
            // Under concurrent interference, Hibernate may raise a serialization /
            // lock exception even though another concurrent operation already removed
            // the message or this user's destination entry. Treat that final state as
            // success.
            try {
                synchronized (getMessageLock(mid)) {
                    Message msg = hibernate.get(Message.class, mid);
                    if (msg == null) {
                        Log.info("removeFromUserInbox -> concurrent delete already completed for: " + mid);
                        return Result.ok();
                    }

                    Set<String> destinations = msg.getDestination() == null
                            ? Set.of()
                            : new HashSet<>(msg.getDestination());
                    if (!destinations.contains(addr)) {
                        Log.info("removeFromUserInbox -> concurrent inbox removal already completed for: " + mid);
                        return Result.ok();
                    }
                }
            } catch (Exception ignored) {
                // Fall through to the original error handling below.
            }

            x.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Deletes a locally stored propagated message.
     *
     * <p>
     * This operation is intended for internal inter-server use and does not
     * require user authentication.
     *
     * @param mid message id
     * @return OK if deleted or absent, or an error result
     */
    public Result<Void> deletePropagatedMessage(String mid) {
        Log.info("deletePropagatedMessage : mid = " + mid);

        if (mid == null) {
            Log.info("Message id is null.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        try {
            synchronized (getMessageLock(mid)) {
                Message msg = hibernate.get(Message.class, mid);
                if (msg != null) {
                    hibernate.delete(msg);
                    originalDestinations.remove(mid);
                }
            }
            return Result.ok();
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Deletes a message sent by the authenticated user and enqueues remote deletion
     * propagation.
     *
     * <p>
     * The method is intentionally asynchronous with respect to remote domains:
     * it deletes the local copy first and only then enqueues one remote delete per
     * remote domain.
     *
     * @param name user name
     * @param mid  message id
     * @param pwd  user password
     * @return OK if the message was deleted or absent, or an error result
     */
    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Log.info("deleteMessage : name = " + name + "; mid = " + mid + "; pwd = " + pwd);

        Result<String> resolved = resolveDeleteMessage(name, pwd, mid);
        if (!resolved.isOK())
            return Result.error(resolved);

        return applyDeleteMessage(resolved.value(), mid);
    }

    /**
     * Authenticates a user and then retrieves a message from that user's inbox.
     *
     * @param name user name
     * @param pwd  user password
     * @param mid  message id
     * @return authorized inbox message, or an error result
     */
    private Result<Message> getAuthorizedInboxMessage(String name, String pwd, String mid) {
        Result<User> auth = authenticateUser(name, pwd);
        if (!auth.isOK())
            return Result.error(auth);

        return getAuthorizedInboxMessage(auth.value(), mid);
    }

    /**
     * Retrieves the authenticated user's inbox message ids, optionally filtering
     * them by subject or contents. Wildcards ({@code ?}) in {@code query} match
     * any single character.
     *
     * @param name  user name
     * @param pwd   user password
     * @param query search text; if blank or null, all inbox ids are returned
     * @return matching inbox message ids, or an error result
     */
    public Result<List<String>> getMessages(String name, String pwd, String query) {
        Log.info("getMessages : name = " + name + "; pwd = " + pwd + "; query = " + query);

        Result<User> auth = authenticateUser(name, pwd);
        if (!auth.isOK())
            return Result.error(auth);

        String addr = userAddress(auth.value());

        try {
            List<String> inboxIds = hibernate.jpql(
                    "SELECT m.id FROM Message m WHERE '" + addr + "' MEMBER OF m.destination",
                    String.class);

            if (query == null || query.isBlank())
                return Result.ok(inboxIds);

            String q = query.toLowerCase();
            List<String> result = new ArrayList<>();
            for (String mid : inboxIds) {
                try {
                    synchronized (getMessageLock(mid)) {
                        Message m = hibernate.get(Message.class, mid);
                        if (m == null)
                            continue;

                        String subject = m.getSubject() == null ? "" : m.getSubject().toLowerCase();
                        String contents = m.getContents() == null ? "" : m.getContents().toLowerCase();
                        if (matchesQuery(subject, q) || matchesQuery(contents, q))
                            result.add(mid);
                    }
                } catch (Exception ignored) {
                    // Message may disappear concurrently while filtering. Ignore and continue.
                }
            }
            return Result.ok(result);
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Searches the authenticated user's inbox for messages matching a query.
     *
     * @param name  user name
     * @param pwd   user password
     * @param query search text
     * @return matching inbox message ids, or an error result
     */
    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        return getMessages(name, pwd, query);
    }

    private boolean matchesQuery(String text, String query) {
        if (text.contains(query))
            return true;

        if (!query.contains("?"))
            return false;

        // Build regex: quote each segment between '?' wildcards and join with '.'
        StringBuilder regexBuilder = new StringBuilder();
        String[] parts = query.split("\\?", -1);
        for (int i = 0; i < parts.length; i++) {
            if (i > 0)
                regexBuilder.append('.');
            regexBuilder.append(Pattern.quote(parts[i]));
        }
        return Pattern.compile(regexBuilder.toString()).matcher(text).find();
    }
}
