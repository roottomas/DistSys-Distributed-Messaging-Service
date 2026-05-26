package sd2526.trab.api.server.common;

import sd2526.trab.api.grpc.DomainUtil;
import sd2526.trab.api.java.Discovery;

import java.net.InetAddress;

public final class ServerSupport {
    private static final String REST_CTX = "/rest";
    private static final String GRPC_CTX = "/grpc";

    /** Shared secret for server-to-server authentication. */
    private static String serverSecret;

    private ServerSupport() {}

    /**
     * Applies JVM runtime settings shared by all server bootstraps.
     */
    public static void initRuntime() {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
    }

    /**
     * Sets the shared secret used for server-to-server authentication.
     */
    public static void setServerSecret(String secret) {
        serverSecret = secret;
    }

    /**
     * Returns the shared secret for server-to-server authentication.
     */
    public static String getServerSecret() {
        return serverSecret;
    }

    /**
     * Returns the host name of the current server process.
     * @return result of the operation.
     */
    public static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Derives the local domain name from the current host name.
     * @return result of the operation.
     */
    public static String localDomain() {
        return DomainUtil.domainFromHost(hostName());
    }

    /**
     * Builds the advertised REST URI for a server.
     *
     * @param host server host name or address.
     * @param port server port.
     * @return result of the operation.
     */
    public static String restUri(String host, int port) {
        return "https://%s:%s%s".formatted(host, port, REST_CTX);
    }

    /**
     * Builds the advertised gRPC URI for a server.
     *
     * @param host server host name or address.
     * @param port server port.
     * @return result of the operation.
     */
    public static String grpcUri(String host, int port) {
        return "grpc://%s:%s%s".formatted(host, port, GRPC_CTX);
    }

    /**
     * Starts discovery.
     *
     * @param serviceName logical service name to advertise in discovery.
     * @param domain domain associated with the service instance.
     * @param serverURI base URI of the remote server.
     * @return result of the operation.
     */
    public static Discovery startDiscovery(String serviceName, String domain, String serverURI) {
        try {
            Discovery discovery = new Discovery(Discovery.DISCOVERY_ADDR, serviceName + "@" + domain, serverURI);
            discovery.start();
            return discovery;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parses extra long fault millis.
     *
     * @param args command-line arguments.
     * @param defaultMillis default retry window in milliseconds.
     * @return result of the operation.
     */
    public static long parseExtraLongFaultMillis(String[] args, long defaultMillis) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("-extralongfault".equals(args[i])) {
                try {
                    return Long.parseLong(args[i + 1]) * 1000L;
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return defaultMillis;
    }

    /**
     * Parses the shared secret from command-line arguments.
     * Expects format: -secret &lt;value&gt;
     */
    public static String parseSecret(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("-secret".equals(args[i])) {
                return args[i + 1];
            }
        }
        return null;
    }
}
