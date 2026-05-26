package sd2526.trab.api.server.grpc;

import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import sd2526.trab.api.User;
import sd2526.trab.api.grpc.DataModelAdaptor;
import sd2526.trab.api.grpc.GrpcUsersGrpc;
import sd2526.trab.api.grpc.Users;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.java.JavaUsers;

/**
 * gRPC adapter for the users service implementation.
 */
public class GrpcUsersController extends GrpcController implements GrpcUsersGrpc.AsyncService, BindableService {
    private volatile JavaUsers impl;

    /**
     * Creates a controller backed by a default {@code JavaUsers} implementation.
     */
    public GrpcUsersController() {
        this.impl = null;
    }

    /**
     * Creates a controller backed by a default {@code JavaUsers} implementation.
     *
     * @param impl service implementation delegated to by this controller.
     */
    public GrpcUsersController(JavaUsers impl) {
        this.impl = impl;
    }

    /**
     * Returns the service implementation used by the controller.
     * @return result of the operation.
     */
    private JavaUsers impl() {
        JavaUsers local = impl;
        if (local == null) {
            synchronized (this) {
                local = impl;
                if (local == null) {
                    local = new JavaUsers(Hibernate.getUsersInstance(), ServerSupport::localDomain);
                    impl = local;
                }
            }
        }
        return local;
    }

    /**
     * Builds the gRPC service definition exposed by this controller.
     * @return result of the operation.
     */
    @Override
    public ServerServiceDefinition bindService() {
        return GrpcUsersGrpc.bindService(this);
    }

    /**
     * Handles the gRPC operation that creates a new user.
     *
     * @param request gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void postUser(Users.GrpcUser request, StreamObserver<Users.PostUserResult> responseObserver) {
        var r = impl().postUser(DataModelAdaptor.GrpcUser_to_User(request));
        if (!r.isOK()) {
            responseObserver.onError(GrpcController.errorCodeToStatus(r.error()));
            return;
        }
        responseObserver.onNext(Users.PostUserResult.newBuilder().setUserAddress(r.value()).build());
        responseObserver.onCompleted();
    }

    /**
     * Handles the gRPC operation that retrieves an authenticated user.
     *
     * @param request gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void getUser(Users.GetUserArgs request, StreamObserver<Users.GetUserResult> responseObserver) {
        var r = impl().getUser(request.getName(), request.getPwd());
        if (!r.isOK()) {
            responseObserver.onError(GrpcController.errorCodeToStatus(r.error()));
            return;
        }
        responseObserver.onNext(Users.GetUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(r.value())).build());
        responseObserver.onCompleted();
    }

    /**
     * Handles the gRPC operation that retrieves a local user without password validation.
     *
     * @param request gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void getInternalUser(Users.GetInternalUserArgs request, StreamObserver<Users.GetUserResult> responseObserver) {
        var r = impl().getInternalUser(request.getName());
        if (!r.isOK()) {
            responseObserver.onError(GrpcController.errorCodeToStatus(r.error()));
            return;
        }
        responseObserver.onNext(Users.GetUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(r.value())).build());
        responseObserver.onCompleted();
    }

    /**
     * Handles the gRPC operation that updates mutable user fields.
     *
     * @param request gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void updateUser(Users.UpdateUserArgs request, StreamObserver<Users.UpdateUserResult> responseObserver) {
        User info = request.hasInfo() ? DataModelAdaptor.GrpcUser_to_User(request.getInfo()) : null;
        var r = impl().updateUser(request.getName(), request.getPwd(), info);
        if (!r.isOK()) {
            responseObserver.onError(GrpcController.errorCodeToStatus(r.error()));
            return;
        }
        responseObserver.onNext(Users.UpdateUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(r.value())).build());
        responseObserver.onCompleted();
    }

    /**
     * Handles the gRPC operation that deletes an authenticated user.
     *
     * @param request gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void deleteUser(Users.DeleteUserArgs request, StreamObserver<Users.DeleteUserResult> responseObserver) {
        var r = impl().deleteUser(request.getName(), request.getPwd());
        if (!r.isOK()) {
            responseObserver.onError(GrpcController.errorCodeToStatus(r.error()));
            return;
        }
        responseObserver.onNext(Users.DeleteUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(r.value())).build());
        responseObserver.onCompleted();
    }

    /**
     * Searches users.
     *
     * @param request gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void searchUsers(Users.SearchUsersArgs request, StreamObserver<Users.GrpcUser> responseObserver) {
        var result = impl().searchUsers(request.getName(), request.getPwd(), request.getQuery());
        if (!result.isOK()) {
            responseObserver.onError(GrpcController.errorCodeToStatus(result.error()));
            return;
        }
        for (User user : result.value())
            responseObserver.onNext(DataModelAdaptor.User_to_GrpcUser(user));
        responseObserver.onCompleted();
    }
}
