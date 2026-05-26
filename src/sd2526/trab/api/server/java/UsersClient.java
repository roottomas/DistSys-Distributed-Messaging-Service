package sd2526.trab.api.server.java;

import sd2526.trab.api.User;
import sd2526.trab.api.java.Result;

/**
 * Internal abstraction used by the messages service to contact a users service.
 *
 * <p>It extends {@link InternalUsers} with the authenticated lookup required
 * when the sender or mailbox owner must be validated.
 */
public interface UsersClient extends InternalUsers {
    /**
     * Retrieves a user after validating the supplied password.
     *
     * @param name user name
     * @param pwd user's password
     * @return authenticated user or an error result
     */
    Result<User> getUser(String name, String pwd);
}
