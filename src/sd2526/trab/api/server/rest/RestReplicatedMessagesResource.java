package sd2526.trab.api.server.rest;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import sd2526.trab.api.Message;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.replication.InterDomainContext;
import sd2526.trab.api.server.replication.ReplicatedMessages;

import java.util.List;

/**
 * JAX-RS resource adapter for the {@link ReplicatedMessages} (Kafka-replicated)
 * implementation.
 *
 * <p>
 * Exposes the {@link RestMessages} REST interface and delegates every call to
 * {@link ReplicatedMessages}, passing version and source-domain headers as
 * needed
 * for linearisable reads and SID-based duplicate detection.
 */
@Singleton
public class RestReplicatedMessagesResource extends RestMessagesBase {
    /** HTTP header used to propagate the required Kafka version to a replica. */
    static final String VERSION_HEADER = "X-MESSAGES-VERSION";
    private final ReplicatedMessages replicatedImpl;

    /**
     * Creates a resource backed by the given {@link ReplicatedMessages} instance.
     *
     * @param impl replicated messages implementation to delegate to
     */
    public RestReplicatedMessagesResource(ReplicatedMessages impl) {
        this.replicatedImpl = impl;
    }

    /**
     * Blocks until this replica has applied all Kafka operations up to the version
     * requested in the {@value VERSION_HEADER} HTTP header.
     * Throws {@code 504 GATEWAY_TIMEOUT} if the replica cannot catch up in time.
     * If no version header is present the method returns immediately.
     */
    private void ensureVersion() {
        if (headers != null) {
            String versionStr = headers.getHeaderString(VERSION_HEADER);
            if (versionStr != null) {
                try {
                    long requiredVersion = Long.parseLong(versionStr);
                    boolean reached = replicatedImpl.waitForVersion(requiredVersion);
                    if (!reached)
                        throw new WebApplicationException(Response.Status.GATEWAY_TIMEOUT);
                } catch (NumberFormatException ignored) {
                }
            }
        }
    }

    /**
     * Reads the SID (Kafka offset) of the originating operation from the
     * {@link InterDomainContext#SID_HEADER} HTTP header.
     *
     * @return the SID as a {@link Long}, or {@code null} if the header is absent
     *         or not a valid number
     */
    private Long readSid() {
        if (headers == null)
            return null;
        String val = headers.getHeaderString(InterDomainContext.SID_HEADER);
        if (val == null || val.isEmpty())
            return null;
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Reads the source domain of the forwarding server from the
     * {@link InterDomainContext#SOURCE_DOMAIN_HEADER} HTTP header.
     *
     * @return the source domain string, or {@code null} if the header is absent
     */
    private String readSourceDomain() {
        if (headers == null)
            return null;
        return headers.getHeaderString(InterDomainContext.SOURCE_DOMAIN_HEADER);
    }

    /**
     * Accepts a message forwarded from a remote domain. Requires the server
     * secret header and uses the SID and source-domain headers for duplicate
     * detection.
     *
     * @param msg forwarded message
     * @return list of unknown local recipients
     */
    @Override
    public List<String> forwardMessage(Message msg) {
        validateSecret();
        return unwrapResultOrThrow(replicatedImpl.forwardMessage(msg, readSid(), readSourceDomain()));
    }

    /**
     * Posts a new message, authenticating the sender with {@code pwd}.
     *
     * @param pwd sender password
     * @param msg message to post
     * @return the new message id
     */
    @Override
    public String postMessage(String pwd, Message msg) {
        return unwrapResultOrThrow(replicatedImpl.postMessage(pwd, msg));
    }

    /**
     * Retrieves a single message from the authenticated user's inbox, waiting
     * for the replica to be sufficiently up to date.
     *
     * @param name user name
     * @param mid  message id
     * @param pwd  user password
     * @return the requested message
     */
    @Override
    public Message getMessage(String name, String mid, String pwd) {
        ensureVersion();
        return unwrapResultOrThrow(replicatedImpl.getInboxMessage(name, mid, pwd));
    }

    /**
     * Returns inbox message ids for the authenticated user, waiting for the replica
     * to be up to date. Supports optional text search via {@code query}.
     *
     * @param name  user name
     * @param pwd   user password
     * @param query optional search string; pass blank or {@code null} for all
     *              messages
     * @return list of matching message ids
     */
    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        ensureVersion();
        return resolveGetMessages(replicatedImpl, name, pwd, query);
    }

    /**
     * Removes the authenticated user's inbox entry for the given message.
     *
     * @param name user name
     * @param mid  message id
     * @param pwd  user password
     */
    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        unwrapVoidResultOrThrow(replicatedImpl.removeInboxMessage(name, mid, pwd));
    }

    /**
     * Deletes a propagated message copy from this domain using SID-based duplicate
     * detection. Requires the server secret header.
     *
     * @param mid message id to delete
     */
    @Override
    public void deletePropagatedMessage(String mid) {
        validateSecret();
        unwrapVoidResultOrThrow(replicatedImpl.deletePropagatedMessage(mid, readSid(), readSourceDomain()));
    }

    /**
     * Deletes a message sent by the authenticated user from all inboxes.
     *
     * @param name user name (must be the original sender)
     * @param mid  message id
     * @param pwd  user password
     */
    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        unwrapVoidResultOrThrow(replicatedImpl.deleteMessage(name, mid, pwd));
    }
}
