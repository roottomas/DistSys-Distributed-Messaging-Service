package sd2526.trab.api.server.java;

/**
 * Factory of clients used to contact the messages service in another domain.
 */
public interface RemoteMessagesClientFactory {
    /**
     * Returns a client bound to the messages service of the given domain.
     *
     * @param domain target domain
     * @return reusable remote client
     */
    RemoteMessagesClient get(String domain);
}
