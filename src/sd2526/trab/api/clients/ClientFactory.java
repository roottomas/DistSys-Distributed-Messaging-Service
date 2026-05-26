package sd2526.trab.api.clients;

import sd2526.trab.api.java.Discovery;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Resolves service endpoints through discovery and caches reusable clients.
 */
public class ClientFactory<T> implements AutoCloseable {
    private static final String REST = "/rest";
    private static final String GRPC = "/grpc";
    private static final String DOMAIN_DELIMITER = "@";

    final Discovery discovery;
    final String serviceName;
    final Function<URI, T> restClientFunc;
    final Function<URI, T> grpcClientFunc;
    final Map<URI, T> clients;
    private final Thread shutdownHook;

    /**
     * Creates a factory bound to one discovery service and one logical service name.
     *
     * @param discovery discovery service used to resolve service instances
     * @param serviceName logical name of the service to resolve
     * @param restClientFunc constructor used for REST endpoints
     * @param grpcClientFunc constructor used for gRPC endpoints
     */
    public ClientFactory(Discovery discovery, String serviceName, Function<URI, T> restClientFunc, Function<URI, T> grpcClientFunc) {
        this.discovery = discovery;
        this.serviceName = serviceName;
        this.restClientFunc = restClientFunc;
        this.grpcClientFunc = grpcClientFunc;
        this.clients = new ConcurrentHashMap<>();
        this.shutdownHook = new Thread(this::closeQuietly, "%s-client-factory-shutdown".formatted(serviceName));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /**
     * Resolves the service endpoint for a domain and returns a cached client for it.
     *
     * @param domain target domain
     * @return client bound to the requested domain
     */
    public T get(String domain) {
        var sn = "%s%s%s".formatted(serviceName, DOMAIN_DELIMITER, domain);
        return newClient(discovery.knownUrisOf(sn, 1)[0]);
    }

    /**
     * Returns a cached client for a concrete server URI, creating it if needed.
     *
     * @param serverURI resolved service URI
     * @return client bound to the requested domain
     */
    public T newClient(URI serverURI) {
        return clients.computeIfAbsent(serverURI, this::createClient);
    }

    /**
     * Builds a transport-specific client for a resolved service URI.
     *
     * @param serverURI resolved service URI
     * @return client bound to the requested domain
     */
    private T createClient(URI serverURI) {
        var path = serverURI.getPath();
        if (path.endsWith(REST)) return restClientFunc.apply(serverURI);
        if (path.endsWith(GRPC)) return grpcClientFunc.apply(serverURI);
        throw new RuntimeException("Unknown service type..." + serverURI);
    }

    /**
     * Closes the operation.
     */
    @Override
    public void close() {
        closeQuietly();
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // JVM is already shutting down.
        }
    }

    /**
     * Closes quietly.
     */
    private void closeQuietly() {
        for (T client : clients.values()) {
            if (client instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception ignored) {
                    // Best effort shutdown.
                }
            }
        }
        clients.clear();
    }
}
