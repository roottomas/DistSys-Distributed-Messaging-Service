package sd2526.trab.api.clients.rest;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.java.RemoteMessagesClient;
import sd2526.trab.api.server.replication.InterDomainContext;

import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

/**
 * REST transport implementation of the messages client contracts.
 */
public class RestMessagesClient extends RestClient implements Messages, RemoteMessagesClient {
    private static final Logger Log = Logger.getLogger(RestMessagesClient.class.getName());

    /**
     * Creates a REST messages client bound to one remote server URI.
     *
     * @param serverURI base URI of the remote server.
     */
    public RestMessagesClient(URI serverURI) {
        super(serverURI, Log);
        target = super.target.path(RestMessages.PATH);
    }

    /**
     * Invokes the REST operation that creates a new outgoing message.
     *
     * @param pwd password used to authenticate the request.
     * @param msg message payload involved in the operation.
     * @return result of the operation.
     */
    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        return super.reTry(() -> super.processResponse(
                target.path("/").queryParam(RestMessages.PWD, pwd).request().accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(msg, MediaType.APPLICATION_JSON)),
                String.class));
    }

    /**
     * Invokes the internal REST operation that processes a message forwarded from
     * another domain.
     *
     * @param msg message payload involved in the operation.
     * @return result of the operation.
     */
    @Override
    public Result<List<String>> forwardMessage(Message msg) {
        return super.reTry(() -> {
            var req = target.path("internal").request().accept(MediaType.APPLICATION_JSON);
            String secret = ServerSupport.getServerSecret();
            if (secret != null)
                req = req.header(RestMessages.SECRET, secret);
            Long sidVal = InterDomainContext.sid.get();
            if (sidVal != null && sidVal >= 0)
                req = req.header(InterDomainContext.SID_HEADER, sidVal);
            String srcDomain = InterDomainContext.sourceDomain.get();
            if (srcDomain != null)
                req = req.header(InterDomainContext.SOURCE_DOMAIN_HEADER, srcDomain);
            try (Response r = req.post(Entity.entity(msg, MediaType.APPLICATION_JSON))) {
                if (r.getStatusInfo().toEnum() == Response.Status.OK)
                    return Result.ok(r.readEntity(new GenericType<>() {
                    }));
                return Result.error(getErrorCodeFrom(r.getStatus()));
            }
        });
    }

    /**
     * Invokes the internal REST operation that removes a propagated copy of a
     * message.
     *
     * @param mid identifier of the target message.
     * @return result of the operation.
     */
    @Override
    public Result<Void> deletePropagatedMessage(String mid) {
        return super.reTry(() -> {
            var req = target.path("internal").path(mid).request();
            String secret = ServerSupport.getServerSecret();
            if (secret != null)
                req = req.header(RestMessages.SECRET, secret);
            Long sidVal = InterDomainContext.sid.get();
            if (sidVal != null && sidVal >= 0)
                req = req.header(InterDomainContext.SID_HEADER, sidVal);
            String srcDomain = InterDomainContext.sourceDomain.get();
            if (srcDomain != null)
                req = req.header(InterDomainContext.SOURCE_DOMAIN_HEADER, srcDomain);
            return super.processResponse(req.delete(), Void.class);
        });
    }

    /**
     * Retrieves one message from an authenticated inbox.
     *
     * @param name user name involved in the operation.
     * @param mid  identifier of the target message.
     * @param pwd  password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        return super.reTry(() -> super.processResponse(
                target.path(RestMessages.MBOX).path(name).path(mid).queryParam(RestMessages.PWD, pwd)
                        .request().accept(MediaType.APPLICATION_JSON).get(),
                Message.class));
    }

    /**
     * Retrieves all message ids currently present in an authenticated inbox.
     *
     * @param name user name involved in the operation.
     * @param pwd  password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        return getMessages(name, pwd, "");
    }

    /**
     * Retrieves inbox message ids, optionally filtered by a search query.
     *
     * @param name  user name involved in the operation.
     * @param pwd   password used to authenticate the request.
     * @param query query string used to filter results.
     * @return result of the operation.
     */
    public Result<List<String>> getMessages(String name, String pwd, String query) {
        return super.reTry(() -> {
            try (Response r = target.path(RestMessages.MBOX).path(name).queryParam(RestMessages.PWD, pwd)
                    .queryParam(RestMessages.QUERY, query).request().accept(MediaType.APPLICATION_JSON).get()) {
                if (r.getStatusInfo().toEnum() == Response.Status.OK)
                    return Result.ok(r.readEntity(new GenericType<>() {
                    }));
                return Result.error(getErrorCodeFrom(r.getStatus()));
            }
        });
    }

    /**
     * Removes one message from an authenticated inbox.
     *
     * @param name user name involved in the operation.
     * @param mid  identifier of the target message.
     * @param pwd  password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        return super.reTry(() -> super.processResponse(
                target.path(RestMessages.MBOX).path(name).path(mid).queryParam(RestMessages.PWD, pwd)
                        .request().delete(),
                Void.class));
    }

    /**
     * Invokes the distributed delete operation for a recently sent message.
     *
     * @param name user name involved in the operation.
     * @param mid  identifier of the target message.
     * @param pwd  password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        return super.reTry(() -> super.processResponse(
                target.path(name).path(mid).queryParam(RestMessages.PWD, pwd).request().delete(),
                Void.class));
    }

    /**
     * Delegates mailbox search to the REST inbox listing endpoint with a non-empty
     * query.
     *
     * @param name  user name involved in the operation.
     * @param pwd   password used to authenticate the request.
     * @param query query string used to filter results.
     * @return result of the operation.
     */
    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        return getMessages(name, pwd, query);
    }
}
