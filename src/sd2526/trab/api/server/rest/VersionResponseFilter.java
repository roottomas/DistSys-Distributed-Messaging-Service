package sd2526.trab.api.server.rest;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import sd2526.trab.api.server.replication.ReplicatedMessages;

/**
 * Response filter that adds the X-MESSAGES-VERSION header to all responses
 * from the replicated messages server.
 */
public class VersionResponseFilter implements ContainerResponseFilter {
    private final ReplicatedMessages replicatedMessages;

    public VersionResponseFilter(ReplicatedMessages replicatedMessages) {
        this.replicatedMessages = replicatedMessages;
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        long version = replicatedMessages.getCurrentVersion();
        responseContext.getHeaders().putSingle(RestReplicatedMessagesResource.VERSION_HEADER, String.valueOf(version));
    }
}
