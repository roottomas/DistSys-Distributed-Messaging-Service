package sd2526.trab.api.server.replication;

/**
 * Thread-local context used to pass the source domain's replication
 * sequence number (sid) through the inter-domain REST call chain.
 *
 * <p>Set by the remote-operation executor thread in {@link sd2526.trab.api.server.java.JavaMessages}
 * before calling the remote Messages client, and read by the REST client
 * to include the sid as a header in the request.
 */
public class InterDomainContext {
    public static final ThreadLocal<Long> sid = new ThreadLocal<>();
    public static final ThreadLocal<String> sourceDomain = new ThreadLocal<>();

    public static final String SID_HEADER = "X-MESSAGES-SID";
    public static final String SOURCE_DOMAIN_HEADER = "X-MESSAGES-SOURCE-DOMAIN";
}
