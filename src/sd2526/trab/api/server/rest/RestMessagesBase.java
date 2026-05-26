package sd2526.trab.api.server.rest;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.server.common.ServerSupport;

import java.util.List;

/**
 * Abstract base for all JAX-RS messages resource adapters.
 *
 * <p>
 * Consolidates the three concerns shared by every messages REST resource:
 * <ul>
 * <li>the injected {@link HttpHeaders} context needed to read request
 * headers;</li>
 * <li>{@link #validateSecret()} — enforces the {@code X-SECRET} header on
 * server-to-server calls;</li>
 * <li>{@link #resolveGetMessages} — the common query-routing logic that
 * dispatches to either {@code getAllInboxMessages} or {@code searchInbox}
 * depending on whether a search query was supplied.</li>
 * </ul>
 */
public abstract class RestMessagesBase extends RestResource implements RestMessages {

    /** JAX-RS-injected HTTP headers for the current request. */
    @Context
    protected HttpHeaders headers;

    /**
     * Validates the {@code X-SECRET} header against the server's configured secret.
     * Throws {@link WebApplicationException} with {@code 403 FORBIDDEN} if the
     * header is absent or does not match. A no-op when no secret is configured.
     */
    protected void validateSecret() {
        String secret = ServerSupport.getServerSecret();
        if (secret != null) {
            String provided = headers != null ? headers.getHeaderString(RestMessages.SECRET) : null;
            if (!secret.equals(provided))
                throw new WebApplicationException(Response.Status.FORBIDDEN);
        }
    }

    /**
     * Dispatches a get-messages request to either the full-inbox listing or the
     * search operation depending on whether {@code query} is blank.
     *
     * @param impl  messages service implementation to delegate to
     * @param name  inbox owner
     * @param pwd   inbox owner's password
     * @param query optional search text; if blank or {@code null} all ids are
     *              returned
     * @return matching message ids
     */
    protected static List<String> resolveGetMessages(Messages impl, String name, String pwd, String query) {
        return unwrapResultOrThrow((query == null || query.isBlank())
                ? impl.getAllInboxMessages(name, pwd)
                : impl.searchInbox(name, pwd, query));
    }
}
