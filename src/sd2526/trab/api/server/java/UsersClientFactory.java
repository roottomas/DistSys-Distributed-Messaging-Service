package sd2526.trab.api.server.java;

/**
 * Factory of clients used to contact the users service of a domain.
 */
public interface UsersClientFactory {
    /**
     * Returns a client bound to the users service of the given domain.
     *
     * @param domain target domain
     * @return reusable users client
     */
    UsersClient get(String domain);
}
