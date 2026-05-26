package sd2526.trab.api.clients.grpc;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.server.common.TLSUtils;

import java.lang.ref.Cleaner;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Base class for all gRPC transport clients.
 *
 * <p>Manages a single {@link ManagedChannel} per instance.  The channel is
 * shut down when {@link #close()} is called explicitly (e.g., via the
 * {@link sd2526.trab.api.clients.ClientFactory} shutdown hook), and also
 * when the client instance itself is garbage-collected — via a
 * {@link Cleaner} safety-net — to avoid the gRPC orphan-channel warning.
 */
public abstract class GrpcClient implements AutoCloseable {
    /**
     * JVM-wide cleaner used to shut down channels whose owning {@code GrpcClient}
     * was never explicitly closed.  A single {@link Cleaner} is shared by all
     * instances to avoid creating excessive daemon threads.
     */
    private static final Cleaner CHANNEL_CLEANER = Cleaner.create();

    protected final ManagedChannel channel;

    /**
     * Handle returned by the {@link Cleaner} registration; calling
     * {@link Cleaner.Cleanable#clean()} is idempotent and runs the cleanup
     * action at most once.
     */
    private final Cleaner.Cleanable cleanable;

    /**
     * Creates a gRPC client connected to the server at the given URI.
     *
     * @param serverURI URI of the remote gRPC server (scheme {@code grpc://}).
     */
    public GrpcClient(URI serverURI) {
        SslContext sslContext = TLSUtils.getGrpcClientSslContext();
        ManagedChannel ch = NettyChannelBuilder.forAddress(serverURI.getHost(), serverURI.getPort())
                .sslContext(sslContext)
                .enableRetry()
                .build();
        this.channel = ch;
        // Safety net: if close() is never called (e.g., the client is only held
        // inside a ConcurrentHashMap that is cleared at JVM shutdown), the Cleaner
        // will still call shutdownNow() before the channel object is collected,
        // preventing the gRPC orphan-channel SEVERE warning.
        // The action captures 'ch' (the ManagedChannel), NOT 'this', so there is no
        // reference cycle that would prevent GrpcClient from being collected.
        this.cleanable = CHANNEL_CLEANER.register(this, ch::shutdownNow);
    }

    /**
     * Maps a gRPC status to the internal service error code.
     *
     * @param status gRPC status returned by a failed call
     * @return result of the operation.
     */
    protected static Result.ErrorCode statusToErrorCode(io.grpc.Status status) {
        if (status == null) return Result.ErrorCode.INTERNAL_ERROR;
        return switch (status.getCode()) {
            case INVALID_ARGUMENT -> Result.ErrorCode.BAD_REQUEST;
            case PERMISSION_DENIED, UNAUTHENTICATED -> Result.ErrorCode.FORBIDDEN;
            case NOT_FOUND -> Result.ErrorCode.NOT_FOUND;
            case ALREADY_EXISTS -> Result.ErrorCode.CONFLICT;
            case UNIMPLEMENTED -> Result.ErrorCode.NOT_IMPLEMENTED;
            case DEADLINE_EXCEEDED, UNAVAILABLE -> Result.ErrorCode.TIMEOUT;
            default -> Result.ErrorCode.INTERNAL_ERROR;
        };
    }

    /**
     * Builds a failed {@link Result} from a gRPC runtime exception.
     *
     * @param e gRPC exception raised by the stub
     * @return result of the operation.
     */
    protected static <T> Result<T> failure(StatusRuntimeException e) {
        return Result.error(statusToErrorCode(e.getStatus()));
    }

    /**
     * Gracefully shuts down the managed gRPC channel, waiting up to 5 seconds for
     * in-flight calls to complete before forcing an immediate shutdown.
     *
     * <p>This method is idempotent: the underlying {@link Cleaner} registration
     * guarantees that the channel is shut down at most once even if both
     * {@code close()} and the GC-triggered safety-net fire.
     */
    @Override
    public void close() {
        // Triggers the cleaner action (ch::shutdownNow) at most once, then waits
        // for the channel to reach terminated state.
        cleanable.clean();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
