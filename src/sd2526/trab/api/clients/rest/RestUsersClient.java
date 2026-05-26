package sd2526.trab.api.clients.rest;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.rest.RestUsers;
import sd2526.trab.api.server.java.UsersClient;

import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

/**
 * REST transport implementation of the users client contracts.
 */
public class RestUsersClient extends RestClient implements Users, UsersClient {
    private static final Logger Log = Logger.getLogger(RestUsersClient.class.getName());

    /**
     * Creates a REST users client bound to one remote server URI.
     *
     * @param serverURI base URI of the remote server.
     */
    public RestUsersClient(URI serverURI) {
        super(serverURI, Log);
        target = super.target.path(RestUsers.PATH);
    }

    /**
     * Invokes the REST operation that creates a new user.
     *
     * @param user user payload involved in the operation.
     * @return result of the operation.
     */
    @Override
    public Result<String> postUser(User user) {
        return super.reTry(() -> super.processResponse(
                target.request().accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(user, MediaType.APPLICATION_JSON)),
                String.class));
    }

    /**
     * Retrieves an authenticated user through the REST users API.
     *
     * @param name user name involved in the operation.
     * @param pwd password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<User> getUser(String name, String pwd) {
        return super.reTry(() -> super.processResponse(
                target.path(name).queryParam(RestUsers.PWD, pwd).request().accept(MediaType.APPLICATION_JSON).get(),
                User.class));
    }

    /**
     * Retrieves a user through the internal no-password REST endpoint.
     *
     * @param name user name involved in the operation.
     * @return result of the operation.
     */
    public Result<User> getInternalUser(String name) {
        return super.reTry(() -> super.processResponse(
                target.path("internal").path(name).request().accept(MediaType.APPLICATION_JSON).get(),
                User.class));
    }

    /**
     * Invokes the REST operation that updates mutable user fields.
     *
     * @param name user name involved in the operation.
     * @param pwd password used to authenticate the request.
     * @param info partial user object with the requested changes.
     * @return result of the operation.
     */
    @Override
    public Result<User> updateUser(String name, String pwd, User info) {
        return super.reTry(() -> super.processResponse(
                target.path(name).queryParam(RestUsers.PWD, pwd).request().accept(MediaType.APPLICATION_JSON)
                        .put(Entity.entity(info, MediaType.APPLICATION_JSON)),
                User.class));
    }

    /**
     * Invokes the REST operation that deletes an authenticated user.
     *
     * @param name user name involved in the operation.
     * @param pwd password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<User> deleteUser(String name, String pwd) {
        return super.reTry(() -> super.processResponse(
                target.path(name).queryParam(RestUsers.PWD, pwd).request().accept(MediaType.APPLICATION_JSON).delete(),
                User.class));
    }

    /**
     * Invokes the REST operation that searches users visible to the caller.
     *
     * @param name user name involved in the operation.
     * @param pwd password used to authenticate the request.
     * @param query query string used to filter results.
     * @return result of the operation.
     */
    @Override
    public Result<List<User>> searchUsers(String name, String pwd, String query) {
        return super.reTry(() -> {
            Response r = target.path("/").queryParam(RestUsers.NAME, name).queryParam(RestUsers.PWD, pwd)
                    .queryParam(RestUsers.QUERY, query).request().accept(MediaType.APPLICATION_JSON).get();
            try {
                if (r.getStatusInfo().toEnum() == Response.Status.OK)
                    return Result.ok(r.readEntity(new GenericType<>() {
                    }));
                return Result.error(getErrorCodeFrom(r.getStatus()));
            } finally { r.close(); }
        });
    }
}
