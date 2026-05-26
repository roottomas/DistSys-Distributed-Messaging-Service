package sd2526.trab.api.grpc;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;

import java.util.HashSet;

/**
 * Converts between the project domain model classes and the protobuf-generated
 * message classes used by gRPC.
 */
public class DataModelAdaptor {

    public static User GrpcUser_to_User(Users.GrpcUser from) {
        return new User(
                emptyToNull(from.getName()),
                from.hasPwd() ? from.getPwd() : null,
                from.hasDisplayName() ? from.getDisplayName() : null,
                from.hasDomain() ? from.getDomain() : null
        );
    }

    public static Users.GrpcUser User_to_GrpcUser(User from) {
        Users.GrpcUser.Builder b = Users.GrpcUser.newBuilder();

        if (from == null)
            return b.build();

        if (from.getName() != null)
            b.setName(from.getName());
        if (from.getPwd() != null)
            b.setPwd(from.getPwd());
        if (from.getDisplayName() != null)
            b.setDisplayName(from.getDisplayName());
        if (from.getDomain() != null)
            b.setDomain(from.getDomain());

        return b.build();
    }

    public static Message GrpcMessage_to_Message(Messages.GrpcMessage from) {
        Message msg = new Message(
                emptyToNull(from.getId()),
                emptyToNull(from.getSender()),
                new HashSet<>(from.getDestinationList()),
                emptyToNull(from.getSubject()),
                emptyToNull(from.getContents())
        );
        if (from.getCreationTime() != 0)
            msg.setCreationTime(from.getCreationTime());
        return msg;
    }

    public static Messages.GrpcMessage Message_to_GrpcMessage(Message from) {
        Messages.GrpcMessage.Builder b = Messages.GrpcMessage.newBuilder();

        if (from == null)
            return b.build();

        if (from.getId() != null)
            b.setId(from.getId());
        if (from.getSender() != null)
            b.setSender(from.getSender());
        if (from.getDestination() != null)
            b.addAllDestination(from.getDestination());
        b.setCreationTime(from.getCreationTime());
        if (from.getSubject() != null)
            b.setSubject(from.getSubject());
        if (from.getContents() != null)
            b.setContents(from.getContents());

        return b.build();
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}
