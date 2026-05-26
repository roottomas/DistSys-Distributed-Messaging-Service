package sd2526.trab.api.server.rest;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import sd2526.trab.api.Message;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.proxy.ProxyMessages;

import java.util.List;

/**
 * JAX-RS resource adapter for the {@link ProxyMessages} (Zoho-backed)
 * implementation.
 *
 * <p>
 * Exposes the {@link RestMessages} REST interface and delegates every call to
 * the
 * underlying {@link ProxyMessages} business-logic layer.
 */
@Singleton
public class RestProxyMessagesResource extends RestMessagesBase {

    private final ProxyMessages impl;

    /**
     * Creates a resource backed by the given {@link ProxyMessages} instance.
     *
     * @param proxyMessages ProxyMessages implementation to delegate to
     */
    public RestProxyMessagesResource(ProxyMessages proxyMessages) {
        this.impl = proxyMessages;
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
        return unwrapResultOrThrow(impl.postMessage(pwd, msg));
    }

    /**
     * Accepts a message forwarded from a remote domain. Requires the server
     * secret header.
     *
     * @param msg forwarded message
     * @return list of unknown local recipients
     */
    @Override
    public List<String> forwardMessage(Message msg) {
        validateSecret();
        return unwrapResultOrThrow(impl.forwardMessage(msg));
    }

    /**
     * Deletes a propagated message copy from this domain. Requires the server
     * secret header.
     *
     * @param mid message id to delete
     */
    @Override
    public void deletePropagatedMessage(String mid) {
        validateSecret();
        unwrapVoidResultOrThrow(impl.deletePropagatedMessage(mid));
    }

    /**
     * Retrieves a single message from the authenticated user's inbox.
     *
     * @param name user name
     * @param mid  message id
     * @param pwd  user password
     * @return the requested message
     */
    @Override
    public Message getMessage(String name, String mid, String pwd) {
        return unwrapResultOrThrow(impl.getInboxMessage(name, mid, pwd));
    }

    /**
     * Returns all inbox message ids for the authenticated user, or only those
     * matching {@code query} when a search term is supplied.
     *
     * @param name  user name
     * @param pwd   user password
     * @param query optional search string; pass blank or {@code null} for all
     *              messages
     * @return list of matching message ids
     */
    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        return resolveGetMessages(impl, name, pwd, query);
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
        unwrapVoidResultOrThrow(impl.removeInboxMessage(name, mid, pwd));
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
        unwrapVoidResultOrThrow(impl.deleteMessage(name, mid, pwd));
    }
}
