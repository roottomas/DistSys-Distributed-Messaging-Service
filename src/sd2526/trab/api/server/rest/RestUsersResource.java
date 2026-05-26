package sd2526.trab.api.server.rest;

import jakarta.inject.Singleton;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.rest.RestUsers;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.java.JavaUsers;

import java.util.List;

/**
 * REST adapter for the users service.
 *
 * <p>This resource only translates HTTP requests into calls to
 * {@link JavaUsers} and converts {@link sd2526.trab.api.java.Result} failures
 * into the corresponding HTTP responses.
 */
@Singleton
public class RestUsersResource extends RestResource implements RestUsers {
    private final JavaUsers impl;

    /**
     * Creates the resource with the default persistence helper and local-domain
     * supplier used by the users service.
     */
    public RestUsersResource() {
        this.impl = new JavaUsers(Hibernate.getUsersInstance(), ServerSupport::localDomain);
    }

    /** {@inheritDoc} */
    @Override
    public String postUser(User user) { return unwrapResultOrThrow(impl.postUser(user)); }
    /** {@inheritDoc} */
    @Override
    public User getUser(String name, String pwd) { return unwrapResultOrThrow(impl.getUser(name, pwd)); }
    /** {@inheritDoc} */
    @Override
    public User updateUser(String name, String pwd, User info) { return unwrapResultOrThrow(impl.updateUser(name, pwd, info)); }
    /** {@inheritDoc} */
    @Override
    public User deleteUser(String name, String pwd) { return unwrapResultOrThrow(impl.deleteUser(name, pwd)); }
    /** {@inheritDoc} */
    @Override
    public User getInternalUser(String name) { return unwrapResultOrThrow(impl.getInternalUser(name)); }
    /** {@inheritDoc} */
    @Override
    public List<User> searchUsers(String name, String pwd, String pattern) { return unwrapResultOrThrow(impl.searchUsers(name, pwd, pattern)); }
}
