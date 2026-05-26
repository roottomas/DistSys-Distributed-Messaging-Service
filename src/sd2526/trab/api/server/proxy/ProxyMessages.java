package sd2526.trab.api.server.proxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Discovery;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.clients.MessagesClientFactory;
import sd2526.trab.api.clients.UsersClientFactory;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.server.java.RemoteMessagesClient;
import sd2526.trab.api.server.java.UsersClient;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Messages service implementation backed by Zoho Mail.
 *
 * <p>
 * Each domain has exactly one user account. All messages are stored as
 * emails sent to a dedicated Zoho mailbox (subject = {@code MSG:<id>},
 * body = base64-encoded JSON of {@link sd2526.trab.api.Message}). An
 * in-memory cache ({@link #messages}) is populated on startup by reading
 * recent emails from the inbox, so normal operations are served from RAM.
 *
 * <p>
 * Remote forwarding and deletion are executed asynchronously by a
 * per-domain single-thread executor so that failures in one domain do not
 * block the caller.
 */
public class ProxyMessages implements Messages {
    private static final Logger Log = Logger.getLogger(ProxyMessages.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long RETRY_DELAY_MILLIS = 1000;
    private static final long USERS_TIMEOUT_MILLIS = 30_000;
    private static final long EXTRA_LONG_FAULT_MILLIS = 90_000;

    private final ZohoMailClient zoho;
    private final UsersClient localUsersClient;
    private final sd2526.trab.api.server.java.RemoteMessagesClientFactory messagesClientFactory;

    // In-memory cache of messages (loaded from Zoho on startup)
    // Map: messageId -> Message
    private final ConcurrentHashMap<String, Message> messages;
    // Map: messageId -> zohoEmailId (for deletion from Zoho)
    private final ConcurrentHashMap<String, String> messageToZohoId;

    private final Map<String, ExecutorService> remoteExecutors;
    private final CountDownLatch loadComplete;

    /**
     * Constructs a {@code ProxyMessages} instance wired to the given Zoho client.
     *
     * <p>
     * If {@code cleanState} is {@code true} the Zoho inbox is cleared and the
     * load is skipped (useful for tests that need a fresh state). Otherwise the
     * constructor kicks off an asynchronous load of existing messages from Zoho,
     * which completes before any public method is served.
     *
     * @param zoho       authenticated Zoho Mail API client
     * @param discovery  multicast discovery used to reach local Users and remote
     *                   Messages servers
     * @param cleanState when {@code true}, clear the Zoho inbox on startup
     * @throws Exception if the Zoho client cannot be initialised
     */
    public ProxyMessages(ZohoMailClient zoho, Discovery discovery, boolean cleanState) throws Exception {
        this.zoho = zoho;
        this.localUsersClient = new UsersClientFactory(discovery).get(ServerSupport.localDomain());
        this.messagesClientFactory = new MessagesClientFactory(discovery);
        this.messages = new ConcurrentHashMap<>();
        this.messageToZohoId = new ConcurrentHashMap<>();
        this.remoteExecutors = new ConcurrentHashMap<>();
        this.loadComplete = new CountDownLatch(1);

        zoho.init();

        if (cleanState) {
            Log.info("Clean state requested - clearing Zoho inbox");
            zoho.clearInbox();
            loadComplete.countDown();
        } else {
            Log.info("Loading existing messages from Zoho inbox");
            new Thread(() -> {
                loadMessagesFromZoho();
                loadComplete.countDown();
            }).start();
        }
    }

    /**
     * Blocks the calling thread until the background Zoho load has finished (or
     * until a 60-second timeout elapses). Called at the start of every read
     * operation so that clients do not see an empty inbox immediately after
     * startup.
     */
    private void awaitLoad() {
        try {
            loadComplete.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Reads all {@code MSG:*} emails sent within the last 10 minutes from the Zoho
     * inbox and populates the in-memory {@link #messages} and
     * {@link #messageToZohoId} caches. Emails in older or unrecognised formats
     * are silently skipped.
     */
    private void loadMessagesFromZoho() {
        try {
            String inboxJson = zoho.getInboxEmails();
            if (inboxJson == null)
                return;

            JsonNode root = mapper.readTree(inboxJson);
            JsonNode data = root.get("data");
            if (data == null || !data.isArray())
                return;

            // Only load emails received in the last 10 minutes (avoids slow fetches for old
            // emails)
            long cutoff = System.currentTimeMillis() - 600_000;

            for (JsonNode email : data) {
                String zohoMsgId = email.get("messageId").asText();
                String folderId = email.has("folderId") ? email.get("folderId").asText() : null;
                String subject = email.has("subject") ? email.get("subject").asText() : "";

                // Only process our stored messages (subject starts with MSG:)
                if (!subject.startsWith("MSG:"))
                    continue;

                // Skip old emails to avoid slow content fetches
                if (email.has("receivedTime")) {
                    long receivedTime = email.get("receivedTime").asLong();
                    if (receivedTime < cutoff)
                        continue;
                }

                // Fetch the full content to get the body
                String content = zoho.getEmail(zohoMsgId, folderId);
                if (content == null)
                    continue;

                try {
                    // The /content endpoint returns JSON: {"data": {"content": "<html>...</html>"}}
                    String bodyHtml = content;
                    try {
                        JsonNode contentRoot = mapper.readTree(content);
                        JsonNode contentData = contentRoot.get("data");
                        if (contentData != null && contentData.has("content")) {
                            bodyHtml = contentData.get("content").asText();
                        }
                    } catch (Exception ignored) {
                        // If not JSON, treat as raw HTML
                    }

                    // Strip HTML tags to get plain text (base64 string)
                    String plainText = bodyHtml.replaceAll("<[^>]+>", "").trim();
                    // Decode HTML entities
                    plainText = plainText.replace("&amp;", "&")
                            .replace("&lt;", "<").replace("&gt;", ">")
                            .replace("&quot;", "\"").replace("&nbsp;", " ").trim();

                    // Only accept base64-encoded messages (current format)
                    String json;
                    try {
                        json = new String(Base64.getDecoder().decode(plainText),
                                java.nio.charset.StandardCharsets.UTF_8);
                    } catch (IllegalArgumentException e) {
                        // Not base64 - old format email, skip it
                        continue;
                    }

                    Message msg = mapper.readValue(json, Message.class);
                    if (msg.getId() != null) {
                        messages.put(msg.getId(), msg);
                        messageToZohoId.put(msg.getId(), zohoMsgId);
                    }
                } catch (Exception e) {
                    Log.warning("Failed to parse stored email as Message: " + e.getMessage());
                }
            }
            Log.info("Loaded " + messages.size() + " messages from Zoho");
        } catch (Exception e) {
            Log.warning("Failed to load messages from Zoho: " + e.getMessage());
        }
    }

    /**
     * Persists {@code msg} to the Zoho inbox by sending a self-email whose
     * subject is {@code MSG:<id>} and whose body is the base64-encoded JSON of
     * {@code msg}. Failures are logged as warnings and do not propagate.
     *
     * @param msg message to persist
     */
    private void storeMessageInZoho(Message msg) {
        try {
            String json = mapper.writeValueAsString(msg);
            String body = Base64.getEncoder().encodeToString(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            boolean ok = zoho.storeMessage("MSG:" + msg.getId(), body);
            if (!ok)
                Log.warning("Failed to store message in Zoho: " + msg.getId());
        } catch (Exception e) {
            Log.warning("Zoho store error: " + e.getMessage());
        }
    }

    /**
     * Removes the Zoho email that backs message {@code mid} from the inbox.
     * The mapping entry in {@link #messageToZohoId} is also removed.
     * If no mapping exists (e.g., the message was never stored in Zoho) the call
     * is a no-op.
     *
     * @param mid message id whose backing email should be deleted
     */
    private void deleteMessageFromZoho(String mid) {
        String zohoId = messageToZohoId.remove(mid);
        if (zohoId != null) {
            try {
                zoho.deleteEmails(List.of(zohoId));
            } catch (Exception e) {
                Log.warning("Zoho delete error: " + e.getMessage());
            }
        }
    }

    // --- Authentication ---

    /**
     * Fetches and authenticates the local user with the given credentials,
     * retrying on transient failures until success or until
     * {@link #USERS_TIMEOUT_MILLIS} elapses.
     *
     * @param name user name
     * @param pwd  user password
     * @return the authenticated {@link User} on success, or an error result
     */
    private Result<User> authenticateUser(String name, String pwd) {
        if (name == null || pwd == null)
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        long deadline = System.currentTimeMillis() + USERS_TIMEOUT_MILLIS;
        while (true) {
            Result<User> r = localUsersClient.getUser(name, pwd);
            if (r.isOK() || (isNotRetriable(r) && r.error() != Result.ErrorCode.TIMEOUT))
                return r;
            if (System.currentTimeMillis() >= deadline)
                return Result.error(Result.ErrorCode.TIMEOUT);
            sleep();
        }
    }

    /**
     * Returns {@code true} when {@code r} represents a terminal (non-retriable)
     * outcome, i.e. the same call should not be repeated.
     *
     * @param r result to inspect
     * @return {@code true} if the result is OK or a permanent error
     */
    private boolean isNotRetriable(Result<?> r) {
        return r == null || r.isOK() ||
                (r.error() != Result.ErrorCode.INTERNAL_ERROR && r.error() != Result.ErrorCode.TIMEOUT);
    }

    /**
     * Pauses the calling thread for {@link #RETRY_DELAY_MILLIS} milliseconds,
     * restoring the interrupt flag if interrupted.
     */
    private void sleep() {
        try {
            Thread.sleep(ProxyMessages.RETRY_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- Address utilities ---

    /**
     * Returns the domain name that this server instance is responsible for.
     *
     * @return local domain string (e.g., {@code "ourorg0"})
     */
    private String localDomain() {
        return ServerSupport.localDomain();
    }

    /**
     * Extracts the domain part from a {@code name@domain} address string.
     *
     * @param addr full address
     * @return domain portion, or {@code null} if the address contains no {@code @}
     */
    private String addressDomain(String addr) {
        int at = addr.indexOf('@');
        return at >= 0 ? addr.substring(at + 1) : null;
    }

    /**
     * Extracts the local part (name) from a {@code name@domain} address string.
     *
     * @param addr full address, or just a name without {@code @}
     * @return name portion before {@code @}, or the whole string if absent
     */
    private String addressName(String addr) {
        int at = addr.indexOf('@');
        return at >= 0 ? addr.substring(0, at) : addr;
    }

    /**
     * Returns the canonical address ({@code name@domain}) of the given user.
     *
     * @param u user
     * @return canonical address string
     */
    private String userAddress(User u) {
        return u.getName() + "@" + u.getDomain();
    }

    /**
     * Formats the sender's display name and address in the form
     * {@code "Display Name <name@domain>"}.
     *
     * @param u sender
     * @return formatted sender string
     */
    private String formatSender(User u) {
        return u.getDisplayName() + " <" + userAddress(u) + ">";
    }

    /**
     * Derives a stable, deterministic ID for {@code msg} by hashing the sender,
     * sorted destinations, subject, and contents into a UUID. Using a
     * deterministic ID prevents duplicate messages when a post is retried.
     *
     * @param msg message for which an ID should be generated
     * @return deterministic UUID string
     */
    private String deterministicId(Message msg) {
        Set<String> sorted = new TreeSet<>(msg.getDestination());
        String fingerprint = msg.getSender() + "|" + sorted + "|" + msg.getSubject() + "|" + msg.getContents();
        return UUID.nameUUIDFromBytes(fingerprint.getBytes()).toString();
    }

    // --- Public API ---

    /**
     * Posts a message from an authenticated local sender to one or more
     * recipients. Local recipients have their copy stored in the Zoho inbox;
     * remote recipients are reached via asynchronous forwarding.
     *
     * @param pwd user password for the sender named in {@code msg.getSender()}
     * @param msg message to send; its {@code id} and {@code sender} fields are
     *            set deterministically by this method
     * @return the stable message id on success, or an error result
     */
    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        if (msg == null || msg.getDestination() == null || msg.getDestination().isEmpty())
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        String senderName = addressName(msg.getSender() != null ? msg.getSender() : "");
        Result<User> authResult = authenticateUser(senderName, pwd);
        if (!authResult.isOK())
            return Result.error(authResult.error());

        User sender = authResult.value();
        msg.setSender(formatSender(sender));

        // Normalize destinations
        Set<String> normalizedDests = new HashSet<>();
        for (String d : msg.getDestination()) {
            if (d.contains("@"))
                normalizedDests.add(d);
            else
                normalizedDests.add(d + "@" + localDomain());
        }
        msg.setDestination(normalizedDests);

        String mid = deterministicId(msg);
        msg.setId(mid);

        // Separate local and remote destinations
        Set<String> localDests = new HashSet<>();
        Map<String, Set<String>> remoteDests = new HashMap<>();

        for (String dest : normalizedDests) {
            String domain = addressDomain(dest);
            if (localDomain().equals(domain)) {
                localDests.add(dest);
            } else {
                remoteDests.computeIfAbsent(domain, k -> new HashSet<>()).add(dest);
            }
        }

        // Store locally for local destinations
        if (!localDests.isEmpty()) {
            Message localCopy = copyMessage(mid, msg, localDests);
            messages.put(mid, localCopy);
            storeMessageInZoho(localCopy);
        }

        // Forward to remote domains asynchronously
        for (var entry : remoteDests.entrySet()) {
            String domain = entry.getKey();
            Set<String> dests = entry.getValue();
            Message remoteCopy = copyMessage(mid, msg, dests);
            submitRemoteForward(domain, remoteCopy, sender);
        }

        return Result.ok(mid);
    }

    /**
     * Receives a message forwarded from a remote domain and stores it locally.
     * Returns an empty list (no unknown local recipients) since the Zoho proxy
     * accepts all forwarded messages without user-existence checks.
     *
     * @param msg forwarded message
     * @return an empty list on success, or an error result if {@code msg} is
     *         invalid
     */
    @Override
    public Result<List<String>> forwardMessage(Message msg) {
        if (msg == null || msg.getId() == null)
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        messages.put(msg.getId(), msg);
        storeMessageInZoho(msg);

        // The Zoho proxy accepts all forwarded messages - return empty unknown-users
        // list
        return Result.ok(List.of());
    }

    /**
     * Removes a message whose deletion was propagated from another domain.
     * If the message is not present the call succeeds silently (idempotent).
     *
     * @param mid message id to delete
     * @return an OK result
     */
    @Override
    public Result<Void> deletePropagatedMessage(String mid) {
        Message removed = messages.remove(mid);
        if (removed != null)
            deleteMessageFromZoho(mid);
        return Result.ok();
    }

    /**
     * Retrieves a single inbox message for the authenticated user.
     *
     * @param name user name
     * @param mid  message id
     * @param pwd  user password
     * @return the message on success, or an error result if not found / forbidden
     */
    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        awaitLoad();
        Result<User> authResult = authenticateUser(name, pwd);
        if (!authResult.isOK())
            return Result.error(authResult.error());

        User user = authResult.value();
        Message msg = messages.get(mid);
        if (msg == null)
            return Result.error(Result.ErrorCode.NOT_FOUND);

        String addr = userAddress(user);
        if (msg.getDestination() == null || !msg.getDestination().contains(addr))
            return Result.error(Result.ErrorCode.NOT_FOUND);

        return Result.ok(msg);
    }

    /**
     * Returns all inbox message ids for the authenticated user.
     *
     * @param name user name
     * @param pwd  user password
     * @return list of message ids, or an error result
     */
    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        awaitLoad();
        Result<User> authResult = authenticateUser(name, pwd);
        if (!authResult.isOK())
            return Result.error(authResult.error());

        User user = authResult.value();
        String addr = userAddress(user);

        List<String> result = new ArrayList<>();
        for (var entry : messages.entrySet()) {
            Message msg = entry.getValue();
            if (msg.getDestination() != null && msg.getDestination().contains(addr)) {
                result.add(entry.getKey());
            }
        }
        return Result.ok(result);
    }

    /**
     * Searches the authenticated user's inbox by subject, contents, or sender
     * (case-insensitive substring match).
     *
     * @param name  user name
     * @param pwd   user password
     * @param query search string; if blank or {@code null}, all messages match
     * @return matching message ids, or an error result
     */
    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        awaitLoad();
        Result<User> authResult = authenticateUser(name, pwd);
        if (!authResult.isOK())
            return Result.error(authResult.error());

        User user = authResult.value();
        String addr = userAddress(user);

        List<String> result = new ArrayList<>();
        for (var entry : messages.entrySet()) {
            Message msg = entry.getValue();
            if (msg.getDestination() != null && msg.getDestination().contains(addr)) {
                if (query == null || query.isBlank() || matchesQuery(msg, query)) {
                    result.add(entry.getKey());
                }
            }
        }
        return Result.ok(result);
    }

    /**
     * Returns {@code true} if the given message's subject, contents, or sender
     * contains {@code query} (case-insensitive substring match).
     *
     * @param msg   message to test
     * @param query search string
     * @return {@code true} if any field of {@code msg} contains {@code query}
     */
    private boolean matchesQuery(Message msg, String query) {
        String q = query.toLowerCase();
        return (msg.getSubject() != null && msg.getSubject().toLowerCase().contains(q)) ||
                (msg.getContents() != null && msg.getContents().toLowerCase().contains(q)) ||
                (msg.getSender() != null && msg.getSender().toLowerCase().contains(q));
    }

    /**
     * Removes the authenticated user's inbox entry for message {@code mid}.
     * If no other recipients remain, the message and its Zoho email are deleted.
     *
     * @param name user name
     * @param mid  message id
     * @param pwd  user password
     * @return OK on success, or an error result
     */
    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Result<User> authResult = authenticateUser(name, pwd);
        if (!authResult.isOK())
            return Result.error(authResult.error());

        User user = authResult.value();
        String addr = userAddress(user);

        Message msg = messages.get(mid);
        if (msg == null)
            return Result.error(Result.ErrorCode.NOT_FOUND);

        if (msg.getDestination() == null || !msg.getDestination().contains(addr))
            return Result.error(Result.ErrorCode.NOT_FOUND);

        // Remove this user from destinations
        msg.getDestination().remove(addr);
        if (msg.getDestination().isEmpty()) {
            messages.remove(mid);
            deleteMessageFromZoho(mid);
        } else {
            storeMessageInZoho(msg); // update
        }
        return Result.ok();
    }

    /**
     * Deletes a message sent by the authenticated user from all inboxes,
     * propagating the deletion to remote domains asynchronously.
     *
     * @param name user name (must be the original sender)
     * @param mid  message id
     * @param pwd  user password
     * @return OK on success, FORBIDDEN if the caller is not the sender, or an
     *         error result otherwise
     */
    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Result<User> authResult = authenticateUser(name, pwd);
        if (!authResult.isOK())
            return Result.error(authResult.error());

        User user = authResult.value();

        Message msg = messages.get(mid);
        if (msg == null)
            return Result.error(Result.ErrorCode.NOT_FOUND);

        // Verify sender owns this message
        String senderAddr = userAddress(user);
        String msgSender = msg.getSender();
        if (msgSender == null || (!msgSender.contains(senderAddr) && !msgSender.equals(name)))
            return Result.error(Result.ErrorCode.FORBIDDEN);

        // Remove locally
        messages.remove(mid);
        deleteMessageFromZoho(mid);

        // Propagate deletion to remote domains
        if (msg.getDestination() != null) {
            Set<String> remoteDomains = new HashSet<>();
            for (String dest : msg.getDestination()) {
                String domain = addressDomain(dest);
                if (domain != null && !localDomain().equals(domain))
                    remoteDomains.add(domain);
            }
            for (String domain : remoteDomains) {
                submitRemoteDelete(domain, mid);
            }
        }
        return Result.ok();
    }

    // --- Remote operations ---

    /**
     * Asynchronously forwards {@code msg} to the Messages server of the given
     * {@code domain}, retrying until success or until
     * {@link #EXTRA_LONG_FAULT_MILLIS} elapses. On timeout, a failure
     * notification message is stored in the sender's inbox.
     *
     * @param domain destination domain
     * @param msg    message to forward (contains only that domain's destinations)
     * @param sender the original sender (used for failure notifications)
     */
    private void submitRemoteForward(String domain, Message msg, User sender) {
        getRemoteExecutor(domain).submit(() -> {
            long deadline = System.currentTimeMillis() + EXTRA_LONG_FAULT_MILLIS;
            while (System.currentTimeMillis() < deadline) {
                try {
                    RemoteMessagesClient client = messagesClientFactory.get(domain);
                    if (client != null) {
                        Result<List<String>> r = client.forwardMessage(msg);
                        if (r.isOK())
                            return;
                        if (isNotRetriable(r))
                            return; // permanent failure
                    }
                } catch (Exception e) {
                    Log.warning("Forward to " + domain + " failed: " + e.getMessage());
                }
                sleep();
            }
            // Timeout - create notification
            createTimeoutNotifications(sender, msg);
        });
    }

    /**
     * Asynchronously propagates deletion of message {@code mid} to the given
     * {@code domain}, retrying until success or until
     * {@link #EXTRA_LONG_FAULT_MILLIS} elapses.
     *
     * @param domain domain to which the delete-propagation should be sent
     * @param mid    message id to delete on the remote server
     */
    private void submitRemoteDelete(String domain, String mid) {
        getRemoteExecutor(domain).submit(() -> {
            long deadline = System.currentTimeMillis() + EXTRA_LONG_FAULT_MILLIS;
            while (System.currentTimeMillis() < deadline) {
                try {
                    RemoteMessagesClient client = messagesClientFactory.get(domain);
                    if (client != null) {
                        Result<Void> r = client.deletePropagatedMessage(mid);
                        if (r.isOK() || isNotRetriable(r))
                            return;
                    }
                } catch (Exception e) {
                    Log.warning("Delete propagation to " + domain + " failed: " + e.getMessage());
                }
                sleep();
            }
        });
    }

    /**
     * Creates a local failure-notification message in the sender's inbox for each
     * destination in {@code msg}, informing the sender that delivery timed out.
     *
     * @param sender the original sender who should receive the notifications
     * @param msg    the message that could not be delivered in time
     */
    private void createTimeoutNotifications(User sender, Message msg) {
        if (msg.getDestination() == null)
            return;
        for (String dest : msg.getDestination()) {
            try {
                Message failure = new Message();
                failure.setId(msg.getId() + "." + dest);
                failure.setSender(msg.getSender());
                failure.setDestination(Set.of(userAddress(sender)));
                failure.setSubject("FAILED TO SEND " + msg.getId() + " TO " + dest + ": TIMEOUT");
                failure.setContents(msg.getContents());
                failure.setCreationTime(msg.getCreationTime());
                messages.put(failure.getId(), failure);
                storeMessageInZoho(failure);
            } catch (Exception e) {
                Log.warning("Failed to create timeout notification: " + e.getMessage());
            }
        }
    }

    /**
     * Returns (or creates) the single-thread {@link ExecutorService} used for
     * remote operations targeting {@code domain}. Using one executor per domain
     * ensures that operations for different domains are independent.
     *
     * @param domain target domain
     * @return executor for that domain
     */
    private ExecutorService getRemoteExecutor(String domain) {
        return remoteExecutors.computeIfAbsent(domain, d -> Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "proxy-remote-" + d);
            t.setDaemon(true);
            return t;
        }));
    }

    /**
     * Creates a shallow copy of {@code source} with a new {@code id} and a
     * specific subset of {@code destinations}. Used to split a multi-domain
     * post into per-domain copies.
     *
     * @param id           new message id
     * @param source       original message to copy fields from
     * @param destinations recipient set for the copy
     * @return a new {@link Message} with the given id and destination set
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
}
