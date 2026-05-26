package sd2526.trab.api.server.grpc;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import sd2526.trab.api.server.common.ServerSupport;

/**
 * Server interceptor that validates the shared secret for internal gRPC methods.
 */
public class SecretInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> SECRET_KEY =
            Metadata.Key.of("X-SECRET", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String methodName = call.getMethodDescriptor().getBareMethodName();

        // Only validate secret for internal methods
        if ("ForwardMessage".equals(methodName) || "DeletePropagatedMessage".equals(methodName)) {
            String secret = ServerSupport.getServerSecret();
            if (secret != null) {
                String provided = headers.get(SECRET_KEY);
                if (!secret.equals(provided)) {
                    call.close(Status.PERMISSION_DENIED.withDescription("Invalid server secret"), new Metadata());
                    return new ServerCall.Listener<>() {};
                }
            }
        }
        return next.startCall(call, headers);
    }
}
