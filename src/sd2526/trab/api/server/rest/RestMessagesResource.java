package sd2526.trab.api.server.rest;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.clients.MessagesClientFactory;
import sd2526.trab.api.clients.UsersClientFactory;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.java.JavaMessages;

import java.util.List;

@Singleton
public class RestMessagesResource extends RestMessagesBase {
    private final JavaMessages impl;

    public RestMessagesResource() {
        this.impl = new JavaMessages(
                Hibernate.getMessagesInstance(),
                ServerSupport::localDomain,
                new UsersClientFactory(RestMessagesServer.discovery).get(ServerSupport.localDomain()),
                new MessagesClientFactory(RestMessagesServer.discovery), RestMessagesServer.EXTRA_LONG_FAULT_MILLIS);
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
     * Creates a new outgoing message on behalf of an authenticated sender.
     *
     * @param pwd sender password
     * @param msg message to create and distribute
     * @return identifier assigned to the message
     */
    @Override
    public String postMessage(String pwd, Message msg) {
        return unwrapResultOrThrow(impl.postMessage(pwd, msg));
    }

    /**
     * Retrieves one message from the authenticated user's inbox.
     *
     * @param name inbox owner
     * @param mid  message identifier
     * @param pwd  password of the inbox owner
     * @return stored message
     */
    @Override
    public Message getMessage(String name, String mid, String pwd) {
        return unwrapResultOrThrow(impl.getInboxMessage(name, mid, pwd));
    }

    /**
     * Lists the ids of the messages currently visible in a user's inbox.
     *
     * <p>
     * If {@code query} is blank the full inbox is returned; otherwise the
     * request is delegated to the mailbox search operation.
     *
     * @param name  inbox owner
     * @param pwd   password of the inbox owner
     * @param query optional mailbox search text
     * @return matching inbox message ids
     */
    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        return resolveGetMessages(impl, name, pwd, query);
    }

    /**
     * Removes one message from a user's inbox.
     *
     * @param name inbox owner
     * @param mid  message identifier
     * @param pwd  password of the inbox owner
     */
    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        unwrapVoidResultOrThrow(impl.removeInboxMessage(name, mid, pwd));
    }

    /**
     * Removes the local copy of a message after a propagated delete request.
     *
     * @param mid message identifier
     */
    @Override
    public void deletePropagatedMessage(String mid) {
        validateSecret();
        unwrapVoidResultOrThrow(impl.deletePropagatedMessage(mid));
    }

    /**
     * Deletes a recently sent message from all places where it is still stored.
     *
     * @param name original sender
     * @param mid  message identifier
     * @param pwd  password of the sender
     */
    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        unwrapVoidResultOrThrow(impl.deleteMessage(name, mid, pwd));
    }
}
