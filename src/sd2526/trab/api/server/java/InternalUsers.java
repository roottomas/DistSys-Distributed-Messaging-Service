package sd2526.trab.api.server.java;

import sd2526.trab.api.User;
import sd2526.trab.api.java.Result;

/**
 * Minimal internal view of the users service.
 *
 * <p>This abstraction is used by the messages service when it needs to validate
 * or retrieve users inside a domain without exposing the full public users API.
 */
public interface InternalUsers {
    /**
     * Retrieves a user without requiring end-user authentication.
     *
     * @param name local user name
     * @return stored user or an error result
     */
    Result<User> getInternalUser(String name);
}
