package sd2526.trab.api.clients;

import sd2526.trab.api.java.Discovery;
import sd2526.trab.api.clients.grpc.GrpcMessagesClient;
import sd2526.trab.api.clients.rest.RestMessagesClient;
import sd2526.trab.api.server.java.RemoteMessagesClient;

/**
 * Domain-aware facade over {@link ClientFactory} for remote messages clients.
 */
public class MessagesClientFactory implements sd2526.trab.api.server.java.RemoteMessagesClientFactory, AutoCloseable {
    private final ClientFactory<RemoteMessagesClient> factory;

    /**
     * Creates a factory that resolves message servers through discovery.
     *
     * @param discovery discovery service used to resolve messages servers
     */
    public MessagesClientFactory(Discovery discovery) {
        this.factory = new ClientFactory<>(discovery, sd2526.trab.api.java.Messages.SERVICE_NAME, RestMessagesClient::new, GrpcMessagesClient::new);
    }

    /**
     * Returns a client for the messages service of a given domain.
     *
     * @param domain target domain
     * @return remote messages client
     */
    @Override
    public RemoteMessagesClient get(String domain) {
        return factory.get(domain);
    }

    /**
     * Closes all cached clients created by the underlying factory.
     */
    @Override
    public void close() {
        factory.close();
    }
}
