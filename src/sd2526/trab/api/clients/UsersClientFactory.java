package sd2526.trab.api.clients;

import sd2526.trab.api.java.Discovery;
import sd2526.trab.api.clients.grpc.GrpcUsersClient;
import sd2526.trab.api.clients.rest.RestUsersClient;
import sd2526.trab.api.server.java.UsersClient;

/**
 * Domain-aware facade over {@link ClientFactory} for users clients.
 */
public class UsersClientFactory implements sd2526.trab.api.server.java.UsersClientFactory, AutoCloseable {
    private final ClientFactory<UsersClient> factory;

    /**
     * Creates a factory that resolves users servers through discovery.
     *
     * @param discovery discovery service used to resolve users servers
     */
    public UsersClientFactory(Discovery discovery) {
        this.factory = new ClientFactory<>(discovery, sd2526.trab.api.java.Users.SERVICE_NAME, RestUsersClient::new, GrpcUsersClient::new);
    }

    /**
     * Returns a client for the users service of a given domain.
     *
     * @param domain target domain
     * @return users client
     */
    @Override
    public UsersClient get(String domain) {
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
