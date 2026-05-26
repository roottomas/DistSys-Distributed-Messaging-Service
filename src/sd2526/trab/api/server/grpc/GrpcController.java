package sd2526.trab.api.server.grpc;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;

import java.util.function.Function;

public abstract class GrpcController {
    /**
     * Maps a service error code to the corresponding gRPC status.
     *
     * @param error service-layer error code.
     * @return result of the operation.
     */
    protected static Throwable errorCodeToStatus(ErrorCode error) {
        var status = switch (error) {
            case NOT_FOUND -> io.grpc.Status.NOT_FOUND;
            case CONFLICT -> io.grpc.Status.ALREADY_EXISTS;
            case FORBIDDEN -> io.grpc.Status.PERMISSION_DENIED;
            case NOT_IMPLEMENTED -> io.grpc.Status.UNIMPLEMENTED;
            case BAD_REQUEST -> io.grpc.Status.INVALID_ARGUMENT;
            case TIMEOUT -> io.grpc.Status.DEADLINE_EXCEEDED;
            default -> io.grpc.Status.INTERNAL;
        };
        return status.asException();
    }

    /**
     * Sends either a mapped success response or a mapped error through the observer.
     *
     * @param observer observer that receives the mapped gRPC response.
     * @param result service-layer result to translate.
     * @param mapper mapper used to convert successful results to gRPC messages.
     */
    protected <T, R> void toGrpcResult(StreamObserver<R> observer, Result<T> result, Function<T, R> mapper) {
        if (!result.isOK()) {
            observer.onError(errorCodeToStatus(result.error()));
            return;
        }
        observer.onNext(mapper.apply(result.value()));
        observer.onCompleted();
    }

    /**
     * Completes a void gRPC call or reports the corresponding error.
     *
     * @param observer observer that receives the mapped gRPC response.
     * @param result service-layer result to translate.
     */
    protected void toGrpcVoid(StreamObserver<Empty> observer, Result<Void> result) {
        if (!result.isOK()) {
            observer.onError(errorCodeToStatus(result.error()));
            return;
        }
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }
}
