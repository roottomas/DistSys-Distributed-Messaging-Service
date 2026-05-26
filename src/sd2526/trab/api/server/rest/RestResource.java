package sd2526.trab.api.server.rest;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import sd2526.trab.api.java.Result;

/**
 * Shared helpers for REST resource classes.
 *
 * <p>The server-side business layer returns {@link Result} objects. REST
 * resources use this class to translate those results into the corresponding
 * HTTP semantics expected by Jersey.
 */
public abstract class RestResource {
    /**
     * Maps a domain-level error code to the HTTP status that should be returned
     * to the client.
     *
     * @param error service-layer error code
     * @return matching HTTP status
     */
    protected static Status errorCodeToStatus(Result.ErrorCode error) {
        return switch (error) {
            case NOT_FOUND -> Status.NOT_FOUND;
            case CONFLICT -> Status.CONFLICT;
            case FORBIDDEN -> Status.FORBIDDEN;
            case NOT_IMPLEMENTED -> Status.NOT_IMPLEMENTED;
            case BAD_REQUEST -> Status.BAD_REQUEST;
            case TIMEOUT -> Status.GATEWAY_TIMEOUT;
            default -> Status.INTERNAL_SERVER_ERROR;
        };
    }

    /**
     * Returns the value inside a successful result or throws a
     * {@link WebApplicationException} with the mapped HTTP status otherwise.
     *
     * @param result service-layer result
     * @return successful result value
     * @param <T> type carried by the result
     */
    protected static <T> T unwrapResultOrThrow(Result<T> result) {
        if (result.isOK()) return result.value();
        throw new WebApplicationException(errorCodeToStatus(result.error()));
    }

    /**
     * Verifies a void result and throws a {@link WebApplicationException} if the
     * operation failed.
     *
     * @param result service-layer result
     */
    protected static void unwrapVoidResultOrThrow(Result<Void> result) {
        if (!result.isOK()) throw new WebApplicationException(errorCodeToStatus(result.error()));
    }
}
