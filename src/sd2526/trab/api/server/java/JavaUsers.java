package sd2526.trab.api.server.java;

import org.hibernate.exception.ConstraintViolationException;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Users;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * In-memory/business implementation of the users service backed by Hibernate.
 *
 * <p>This class centralizes validation, authentication and persistence logic
 * for local user accounts. Transport-specific adapters delegate directly to it.
 */
public class JavaUsers implements Users, sd2526.trab.api.server.java.InternalUsers {

    private static final Logger Log = Logger.getLogger(JavaUsers.class.getName());

    private final Hibernate hibernate;
    private final Supplier<String> localDomainSupplier;

    /**
     * Creates a users service using the default users persistence instance and
     * a local-domain supplier that returns null.
     */
    public JavaUsers() {
        this(Hibernate.getUsersInstance(), () -> null);
    }

    /**
     * Creates a users service with the given persistence layer and local-domain supplier.
     * @param hibernate the persistence helper used to access stored users.
     * @param localDomainSupplier a supplier that returns the local domain of the service.
     */
    public JavaUsers(Hibernate hibernate, Supplier<String> localDomainSupplier) {
        this.hibernate = hibernate;
        this.localDomainSupplier = localDomainSupplier;
    }

    /**
     * Gets the local domain from the configured supplier.
     * @return the local domain, or null if the supplier fails.
     */
    private String localDomain() {
        try {
            return localDomainSupplier.get();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Authenticates a user by validating the provided name and password against
     * the locally stored user record.
     * @param name the user's name.
     * @param pwd the user's password.
     * @return the authenticated user if the credentials are valid,
     *         BAD_REQUEST if name or password is null,
     *         FORBIDDEN if the user does not exist or the password is incorrect,
     *         or INTERNAL_ERROR if an unexpected persistence error occurs.
     */
    public Result<User> authenticate(String name, String pwd) {
        if (name == null || pwd == null) {
            Log.info("Name or password null.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        try {
            Log.info("[JavaUsers] authenticate using hibernate=" + hibernate + "; name=" + name);
            User user = hibernate.get(User.class, name);
            if (user == null || !user.getPwd().equals(pwd)) {
                Log.info("User does not exist or password is incorrect.");
                return Result.error(Result.ErrorCode.FORBIDDEN);
            }
            return Result.ok(user);
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Retrieves a locally stored user without requiring authentication.
     * This method is intended for internal service use.
     * @param name the name of the user to retrieve.
     * @return the user if found,
     *         BAD_REQUEST if the name is null,
     *         NOT_FOUND if no such user exists,
     *         or INTERNAL_ERROR if an unexpected persistence error occurs.
     */
    public Result<User> getInternalUser(String name) {
        Log.info("getInternalUser : name = " + name);

        if (name == null) {
            Log.info("Name is null.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        try {
            Log.info("[JavaUsers] authenticate using hibernate=" + hibernate + "; name=" + name);
            User user = hibernate.get(User.class, name);
            if (user == null) {
                Log.info("User not found.");
                return Result.error(Result.ErrorCode.NOT_FOUND);
            }
            return Result.ok(user);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Creates a new user in the local repository.
     * The provided user must contain all required fields. If a local domain is
     * configured, the user's domain must match it. If a user with the same name
     * already exists and all stored data matches, the operation is treated as
     * idempotent and succeeds. If the stored data differs, the operation fails
     * with CONFLICT.
     * @param user the user to create.
     * @return the full user address in the form name@domain if creation succeeds,
     *         BAD_REQUEST if the user or required fields are null,
     *         FORBIDDEN if the user's domain does not match the local domain,
     *         CONFLICT if a different user with the same name already exists,
     *         or INTERNAL_ERROR if an unexpected persistence error occurs.
     */
    @Override
    public Result<String> postUser(User user) {
        Log.info("postUser : " + user);

        if (user == null || user.getName() == null || user.getPwd() == null
                || user.getDisplayName() == null || user.getDomain() == null) {
            Log.info("User object invalid.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        String localDomain = localDomain();
        if (localDomain != null && !localDomain.equals(user.getDomain())) {
            Log.info("User domain does not match local domain.");
            return Result.error(Result.ErrorCode.FORBIDDEN);
        }

        try {
            Log.info("[JavaUsers] postUser localDomain=" + localDomain + "; user.domain=" + user.getDomain());
            Log.info("[JavaUsers] postUser checking existing user in hibernate=" + hibernate);
            User existing = hibernate.get(User.class, user.getName());

            if (existing != null) {
                boolean sameUser =
                        existing.getName().equals(user.getName()) &&
                                existing.getPwd().equals(user.getPwd()) &&
                                existing.getDisplayName().equals(user.getDisplayName()) &&
                                existing.getDomain().equals(user.getDomain());

                if (sameUser)
                    return Result.ok(user.getName() + "@" + user.getDomain());

                Log.info("User already exists with different data.");
                return Result.error(Result.ErrorCode.CONFLICT);
            }

            Log.info("[JavaUsers] postUser persisting user=" + user);
            hibernate.persist(user);
            Log.info("[JavaUsers] postUser persisted successfully user=" + user.getName());
            return Result.ok(user.getName() + "@" + user.getDomain());
        } catch (ConstraintViolationException e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.CONFLICT);
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Retrieves a user after authenticating the provided credentials.
     * @param name the user's name.
     * @param pwd the user's password.
     * @return the authenticated user if the credentials are valid,
     *         or an error result propagated from authentication.
     */
    @Override
    public Result<User> getUser(String name, String pwd) {
        Log.info("getUser : name = " + name + "; pwd = " + pwd);
        return authenticate(name, pwd);
    }

    /**
     * Updates the authenticated user's mutable information.
     * The user's name cannot be changed. Any non-null fields in {@code info}
     * are applied to the stored user record.
     * @param name the name of the user to update.
     * @param pwd the user's password for authentication.
     * @param info an object containing the new values for mutable fields.
     * @return the updated user if the operation succeeds,
     *         BAD_REQUEST if name, password, or info is null, or if info attempts
     *         to change the user's name,
     *         or an error result propagated from authentication or persistence.
     */
    @Override
    public Result<User> updateUser(String name, String pwd, User info) {
        Log.info("updateUser : name = " + name + "; pwd = " + pwd + "; info = " + info);

        if (name == null || pwd == null || info == null) {
            Log.info("Name, password or info null.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        if (info.getName() != null) {
            Log.info("User name cannot be changed.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        Result<User> auth = authenticate(name, pwd);
        if (!auth.isOK())
            return Result.error(auth);

        User user = auth.value();

        if (info.getPwd() != null)
            user.setPwd(info.getPwd());

        if (info.getDisplayName() != null)
            user.setDisplayName(info.getDisplayName());

        if (info.getDomain() != null)
            user.setDomain(info.getDomain());

        try {
            hibernate.update(user);
            return Result.ok(user);
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Deletes the authenticated user from the local repository.
     * @param name the name of the user to delete.
     * @param pwd the user's password for authentication.
     * @return the deleted user if the operation succeeds,
     *         or an error result propagated from authentication or persistence.
     */
    @Override
    public Result<User> deleteUser(String name, String pwd) {
        Log.info("deleteUser : name = " + name + "; pwd = " + pwd);

        Result<User> auth = authenticate(name, pwd);
        if (!auth.isOK())
            return Result.error(auth);

        try {
            hibernate.delete(auth.value());
            return Result.ok(auth.value());
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    /**
     * Searches for users whose names contain the given query string.
     * The requester must be authenticated. Returned users are sanitized so that
     * their password field is not exposed.
     * @param name the name of the user performing the search.
     * @param pwd the requester's password for authentication.
     * @param query the case-insensitive substring to match against usernames.
     * @return the list of matching users with blank passwords,
     *         BAD_REQUEST if the query is null,
     *         or an error result propagated from authentication or persistence.
     */
    @Override
    public Result<List<User>> searchUsers(String name, String pwd, String query) {
        Log.info("searchUsers : name = " + name + "; pwd = " + pwd + "; pattern = " + query);

        Result<User> auth = authenticate(name, pwd);
        if (!auth.isOK())
            return Result.error(auth);

        if (query == null) {
            Log.info("Pattern is null.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        try {
            String p = query.toLowerCase();
            List<User> users = hibernate.jpql(
                    "SELECT u FROM User u WHERE LOWER(u.name) LIKE '%" + p + "%'",
                    User.class);

            List<User> result = new ArrayList<>();
            for (User u : users)
                result.add(new User(u.getName(), "", u.getDisplayName(), u.getDomain()));

            return Result.ok(result);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }
}
