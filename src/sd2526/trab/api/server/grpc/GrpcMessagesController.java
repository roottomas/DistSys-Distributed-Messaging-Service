package sd2526.trab.api.server.grpc;

import com.google.protobuf.Empty;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import sd2526.trab.api.Message;
import sd2526.trab.api.grpc.DataModelAdaptor;
import sd2526.trab.api.grpc.GrpcMessagesGrpc;
import sd2526.trab.api.grpc.Messages;
import sd2526.trab.api.server.java.JavaMessages;

/**
 * gRPC service adapter for the messages service implementation.
 *
 * <p>
 * Each gRPC method delegates to the underlying {@link JavaMessages}
 * business-logic layer, converting between Protobuf request/response messages
 * and domain model objects via {@link DataModelAdaptor}.
 */
public class GrpcMessagesController extends GrpcController implements GrpcMessagesGrpc.AsyncService, BindableService {
    private final JavaMessages impl;

    /**
     * Creates a controller backed by the given {@link JavaMessages} implementation.
     *
     * @param impl messages service implementation to delegate to
     */
    public GrpcMessagesController(JavaMessages impl) {
        this.impl = impl;
    }

    /**
     * Builds the gRPC service definition exposed by this controller.
     *
     * @return the bound {@link ServerServiceDefinition}
     */
    @Override
    public ServerServiceDefinition bindService() {
        return GrpcMessagesGrpc.bindService(this);
    }

    /**
     * Handles the gRPC operation that posts a new message.
     *
     * @param request          gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void postMessage(Messages.PostMessageArgs request,
            StreamObserver<Messages.PostMessageResult> responseObserver) {
        Message msg = request.hasMessage() ? DataModelAdaptor.GrpcMessage_to_Message(request.getMessage()) : null;
        super.toGrpcResult(
                responseObserver,
                impl.postMessage(request.getPwd(), msg),
                mid -> Messages.PostMessageResult.newBuilder().setMid(mid).build());
    }

    /**
     * Handles the gRPC operation that forwards a message from a remote domain.
     *
     * @param request          gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void forwardMessage(Messages.GrpcMessage request,
            StreamObserver<Messages.SearchInboxResult> responseObserver) {
        super.toGrpcResult(
                responseObserver,
                impl.forwardMessage(DataModelAdaptor.GrpcMessage_to_Message(request)),
                mids -> Messages.SearchInboxResult.newBuilder().addAllMids(mids).build());
    }

    /**
     * Handles the gRPC operation that deletes a propagated message copy in this
     * domain.
     *
     * @param request          gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void deletePropagatedMessage(Messages.PostMessageResult request, StreamObserver<Empty> responseObserver) {
        super.toGrpcVoid(responseObserver, impl.deletePropagatedMessage(emptyToNull(request.getMid())));
    }

    /**
     * Handles the gRPC operation that retrieves one message from an inbox.
     *
     * @param request          gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void getInboxMessage(Messages.GetInboxMessageArgs request,
            StreamObserver<Messages.GrpcMessage> responseObserver) {
        super.toGrpcResult(
                responseObserver,
                impl.getInboxMessage(request.getName(), emptyToNull(request.getMid()), request.getPwd()),
                DataModelAdaptor::Message_to_GrpcMessage);
    }

    /**
     * Handles the gRPC operation that lists all message ids in an inbox.
     *
     * @param request          gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void getAllInboxMessages(Messages.GetAllInboxMessagesArgs request,
            StreamObserver<Messages.GetAllInboxMessagesResult> responseObserver) {
        super.toGrpcResult(
                responseObserver,
                impl.getAllInboxMessages(request.getName(), request.getPwd()),
                mids -> Messages.GetAllInboxMessagesResult.newBuilder().addAllMids(mids).build());
    }

    /**
     * Handles the gRPC operation that removes one message from an inbox.
     *
     * @param request          gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void removeInboxMessage(Messages.RemoveInboxMessageArgs request, StreamObserver<Empty> responseObserver) {
        super.toGrpcVoid(
                responseObserver,
                impl.removeInboxMessage(request.getName(), emptyToNull(request.getMid()), request.getPwd()));
    }

    /**
     * Handles the gRPC operation that deletes a message sent by the given user.
     *
     * @param request          gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void deleteMessage(Messages.DeleteMessageArgs request, StreamObserver<Empty> responseObserver) {
        super.toGrpcVoid(
                responseObserver,
                impl.deleteMessage(request.getName(), emptyToNull(request.getMid()), request.getPwd()));
    }

    /**
     * Handles the gRPC operation that searches a mailbox by text.
     *
     * @param request          gRPC request message.
     * @param responseObserver stream observer used to return the response.
     */
    @Override
    public void searchInbox(Messages.SearchInboxArgs request,
            StreamObserver<Messages.SearchInboxResult> responseObserver) {
        super.toGrpcResult(
                responseObserver,
                impl.searchInbox(request.getName(), request.getPwd(), request.getQuery()),
                mids -> Messages.SearchInboxResult.newBuilder().addAllMids(mids).build());
    }

    /**
     * Converts empty strings from gRPC requests into {@code null}.
     *
     * @param s string to normalize.
     * @return result of the operation.
     */
    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }
}
