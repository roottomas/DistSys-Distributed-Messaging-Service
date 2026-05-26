package sd2526.trab.api.clients.grpc;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import sd2526.trab.api.Message;
import sd2526.trab.api.grpc.DataModelAdaptor;
import sd2526.trab.api.grpc.GrpcMessagesGrpc;
import sd2526.trab.api.grpc.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.server.common.ServerSupport;

import java.net.URI;
import java.util.List;

public class GrpcMessagesClient extends GrpcClient
        implements sd2526.trab.api.java.Messages, sd2526.trab.api.server.java.RemoteMessagesClient {
    private static final Metadata.Key<String> SECRET_KEY = Metadata.Key.of("X-SECRET",
            Metadata.ASCII_STRING_MARSHALLER);

    private final GrpcMessagesGrpc.GrpcMessagesBlockingStub stub;

    public GrpcMessagesClient(URI serverURI) {
        super(serverURI);
        this.stub = GrpcMessagesGrpc.newBlockingStub(channel);
    }

    private GrpcMessagesGrpc.GrpcMessagesBlockingStub stubWithSecret() {
        String secret = ServerSupport.getServerSecret();
        if (secret == null)
            return stub;
        Metadata metadata = new Metadata();
        metadata.put(SECRET_KEY, secret);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    /**
     * Invokes the gRPC operation that creates a new outgoing message.
     *
     * @param pwd password used to authenticate the request.
     * @param msg message payload involved in the operation.
     * @return result of the operation.
     */
    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        try {
            Messages.PostMessageArgs.Builder b = Messages.PostMessageArgs.newBuilder();
            if (pwd != null)
                b.setPwd(pwd);
            if (msg != null)
                b.setMessage(DataModelAdaptor.Message_to_GrpcMessage(msg));
            return Result.ok(stub.postMessage(b.build()).getMid());
        } catch (StatusRuntimeException e) {
            return failure(e);
        }
    }

    /**
     * Invokes the gRPC operation that processes a message forwarded from another
     * domain.
     *
     * @param msg message payload involved in the operation.
     * @return result of the operation.
     */
    @Override
    public Result<List<String>> forwardMessage(Message msg) {
        try {
            return Result
                    .ok(stubWithSecret().forwardMessage(DataModelAdaptor.Message_to_GrpcMessage(msg)).getMidsList());
        } catch (StatusRuntimeException e) {
            return failure(e);
        }
    }

    /**
     * Invokes the gRPC operation that removes a propagated message copy.
     *
     * @param mid identifier of the target message.
     * @return result of the operation.
     */
    @Override
    public Result<Void> deletePropagatedMessage(String mid) {
        try {
            stubWithSecret().deletePropagatedMessage(Messages.PostMessageResult.newBuilder().setMid(mid).build());
            return Result.ok();
        } catch (StatusRuntimeException e) {
            return failure(e);
        }
    }

    /**
     * Retrieves one message from an authenticated inbox through gRPC.
     *
     * @param name user name involved in the operation.
     * @param mid  identifier of the target message.
     * @param pwd  password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        try {
            Messages.GetInboxMessageArgs.Builder b = Messages.GetInboxMessageArgs.newBuilder();
            if (name != null)
                b.setName(name);
            if (mid != null)
                b.setMid(mid);
            if (pwd != null)
                b.setPwd(pwd);
            return Result.ok(DataModelAdaptor.GrpcMessage_to_Message(stub.getInboxMessage(b.build())));
        } catch (StatusRuntimeException e) {
            return failure(e);
        }
    }

    /**
     * Retrieves all message ids currently present in an authenticated inbox.
     *
     * @param name user name involved in the operation.
     * @param pwd  password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        try {
            Messages.GetAllInboxMessagesArgs.Builder b = Messages.GetAllInboxMessagesArgs.newBuilder();
            if (name != null)
                b.setName(name);
            if (pwd != null)
                b.setPwd(pwd);
            return Result.ok(stub.getAllInboxMessages(b.build()).getMidsList());
        } catch (StatusRuntimeException e) {
            return failure(e);
        }
    }

    /**
     * Removes one message from an authenticated inbox through gRPC.
     *
     * @param name user name involved in the operation.
     * @param mid  identifier of the target message.
     * @param pwd  password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        try {
            Messages.RemoveInboxMessageArgs.Builder b = Messages.RemoveInboxMessageArgs.newBuilder();
            if (name != null)
                b.setName(name);
            if (mid != null)
                b.setMid(mid);
            if (pwd != null)
                b.setPwd(pwd);
            stub.removeInboxMessage(b.build());
            return Result.ok();
        } catch (StatusRuntimeException e) {
            return failure(e);
        }
    }

    /**
     * Invokes the distributed delete operation through gRPC.
     *
     * @param name user name involved in the operation.
     * @param mid  identifier of the target message.
     * @param pwd  password used to authenticate the request.
     * @return result of the operation.
     */
    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        try {
            Messages.DeleteMessageArgs.Builder b = Messages.DeleteMessageArgs.newBuilder();
            if (name != null)
                b.setName(name);
            if (mid != null)
                b.setMid(mid);
            if (pwd != null)
                b.setPwd(pwd);
            stub.deleteMessage(b.build());
            return Result.ok();
        } catch (StatusRuntimeException e) {
            return failure(e);
        }
    }

    /**
     * Invokes the gRPC mailbox search operation.
     *
     * @param name  user name involved in the operation.
     * @param pwd   password used to authenticate the request.
     * @param query query string used to filter results.
     * @return result of the operation.
     */
    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        try {
            Messages.SearchInboxArgs.Builder b = Messages.SearchInboxArgs.newBuilder();
            if (name != null)
                b.setName(name);
            if (pwd != null)
                b.setPwd(pwd);
            if (query != null)
                b.setQuery(query);
            return Result.ok(stub.searchInbox(b.build()).getMidsList());
        } catch (StatusRuntimeException e) {
            return failure(e);
        }
    }
}
