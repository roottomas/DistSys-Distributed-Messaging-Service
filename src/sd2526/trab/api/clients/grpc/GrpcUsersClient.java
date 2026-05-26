package sd2526.trab.api.clients.grpc;

import io.grpc.StatusRuntimeException;
import sd2526.trab.api.User;
import sd2526.trab.api.grpc.DataModelAdaptor;
import sd2526.trab.api.grpc.GrpcUsersGrpc;
import sd2526.trab.api.grpc.Users;
import sd2526.trab.api.java.Result;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * gRPC transport implementation of the users client contracts.
 */
public class GrpcUsersClient extends GrpcClient implements sd2526.trab.api.java.Users, sd2526.trab.api.server.java.UsersClient {
    private final GrpcUsersGrpc.GrpcUsersBlockingStub stub;

    /**
     * Creates a gRPC users client bound to one remote server URI.
     *
     * @param serverURI base URI of the remote server.
     */
    public GrpcUsersClient(URI serverURI) {
        super(serverURI);
        this.stub = GrpcUsersGrpc.newBlockingStub(channel);
    }

    /**
     * Invokes the gRPC operation that creates a new user.
     *
     * @param user user payload involved in the operation.
     * @return result of the operation.
     */
    @Override
    public Result<String> postUser(User user) {
        try { return Result.ok(stub.postUser(DataModelAdaptor.User_to_GrpcUser(user)).getUserAddress()); }
        catch (StatusRuntimeException e) { return failure(e); }
    }

    /**
     * Retrieves an authenticated user through gRPC.
     *
     * @param name user name involved in the operation.
     * @param pwd password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<User> getUser(String name, String pwd) {
        try {
            Users.GetUserArgs.Builder b = Users.GetUserArgs.newBuilder();
            if (name != null) b.setName(name);
            if (pwd != null) b.setPwd(pwd);
            return Result.ok(DataModelAdaptor.GrpcUser_to_User(stub.getUser(b.build()).getUser()));
        } catch (StatusRuntimeException e) { return failure(e); }
    }

    /**
     * Retrieves a user through the internal no-password gRPC endpoint.
     *
     * @param name user name involved in the operation.
     * @return result of the operation.
     */
    public Result<User> getInternalUser(String name) {
        try {
            Users.GetInternalUserArgs.Builder b = Users.GetInternalUserArgs.newBuilder();
            if (name != null) b.setName(name);
            return Result.ok(DataModelAdaptor.GrpcUser_to_User(stub.getInternalUser(b.build()).getUser()));
        } catch (StatusRuntimeException e) { return failure(e); }
    }

    /**
     * Invokes the gRPC operation that updates mutable user fields.
     *
     * @param name user name involved in the operation.
     * @param pwd password used to authenticate the request.
     * @param info partial user object with the requested changes.
     * @return result of the operation.
     */
    @Override
    public Result<User> updateUser(String name, String pwd, User info) {
        try {
            Users.UpdateUserArgs.Builder b = Users.UpdateUserArgs.newBuilder();
            if (name != null) b.setName(name);
            if (pwd != null) b.setPwd(pwd);
            if (info != null) b.setInfo(DataModelAdaptor.User_to_GrpcUser(info));
            return Result.ok(DataModelAdaptor.GrpcUser_to_User(stub.updateUser(b.build()).getUser()));
        } catch (StatusRuntimeException e) { return failure(e); }
    }

    /**
     * Invokes the gRPC operation that deletes an authenticated user.
     *
     * @param name user name involved in the operation.
     * @param pwd password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<User> deleteUser(String name, String pwd) {
        try {
            Users.DeleteUserArgs.Builder b = Users.DeleteUserArgs.newBuilder();
            if (name != null) b.setName(name);
            if (pwd != null) b.setPwd(pwd);
            return Result.ok(DataModelAdaptor.GrpcUser_to_User(stub.deleteUser(b.build()).getUser()));
        } catch (StatusRuntimeException e) { return failure(e); }
    }

    /**
     * Invokes the gRPC operation that searches users visible to the caller.
     *
     * @param name user name involved in the operation.
     * @param pwd password used to authenticate the request.
     * @param query query string used to filter results.
     * @return result of the operation.
     */
    @Override
    public Result<List<User>> searchUsers(String name, String pwd, String query) {
        try {
            Users.SearchUsersArgs.Builder b = Users.SearchUsersArgs.newBuilder();
            if (name != null) b.setName(name);
            if (pwd != null) b.setPwd(pwd);
            if (query != null) b.setQuery(query);
            Iterator<Users.GrpcUser> it = stub.searchUsers(b.build());
            List<User> users = new ArrayList<>();
            while (it.hasNext()) users.add(DataModelAdaptor.GrpcUser_to_User(it.next()));
            return Result.ok(users);
        } catch (StatusRuntimeException e) { return failure(e); }
    }
}
