package sd2526.trab.api.clients.rest;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.server.common.TLSUtils;

import java.net.URI;
import java.util.function.Supplier;
import java.util.logging.Logger;

public abstract class RestClient {
    private static final int READ_TIMEOUT = 5000;
    private static final int CONNECT_TIMEOUT = 5000;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_SLEEP = 1000;

    protected final Logger logger;
    protected final URI serverURI;
    protected final Client client;
    protected final ClientConfig config;
    protected WebTarget target;

    public RestClient(URI serverURI, Logger logger) {
        this.serverURI = serverURI;
        this.logger = logger;
        this.config = new ClientConfig();
        this.config.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
        this.config.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        this.client = ClientBuilder.newBuilder()
                .withConfig(config)
                .sslContext(TLSUtils.getClientSSLContext())
                .hostnameVerifier((hostname, session) -> true)
                .build();
        this.target = client.target(serverURI);
    }

    /**
     * Executes a REST call with a small retry policy for transient processing failures.
     *
     * @param func REST call to execute
     * @return result returned by the supplied call
     */
    protected <T> Result<T> reTry(Supplier<Result<T>> func) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            try { return func.get(); }
            catch (ProcessingException x) {
                try { Thread.sleep(RETRY_SLEEP); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return Result.error(Result.ErrorCode.INTERNAL_ERROR); }
            } catch (Exception x) {
                return Result.error(Result.ErrorCode.INTERNAL_ERROR);
            }
        }
        return Result.error(Result.ErrorCode.TIMEOUT);
    }

    /**
     * Maps an HTTP status code to the internal service error code.
     *
     * @param statusCode HTTP status code
     * @return result returned by the supplied call
     */
    protected static Result.ErrorCode getErrorCodeFrom(int statusCode) {
        return switch (statusCode) {
            case 400 -> Result.ErrorCode.BAD_REQUEST;
            case 403 -> Result.ErrorCode.FORBIDDEN;
            case 404 -> Result.ErrorCode.NOT_FOUND;
            case 409 -> Result.ErrorCode.CONFLICT;
            case 501 -> Result.ErrorCode.NOT_IMPLEMENTED;
            case 504 -> Result.ErrorCode.TIMEOUT;
            default -> Result.ErrorCode.INTERNAL_ERROR;
        };
    }

    /**
     * Converts an HTTP response into the internal {@link Result} representation.
     *
     * @param r HTTP response
     * @param entityType entity class expected in successful responses
     * @return result returned by the supplied call
     */
    protected <T> Result<T> processResponse(Response r, Class<T> entityType) {
        try {
            var status = r.getStatusInfo().toEnum();
            if (status == Status.OK && r.hasEntity()) return Result.ok(r.readEntity(entityType));
            if (status == Status.NO_CONTENT) return Result.ok();
            return Result.error(getErrorCodeFrom(status.getStatusCode()));
        } finally {
            r.close();
        }
    }
}
