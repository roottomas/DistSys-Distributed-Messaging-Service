package sd2526.trab.api.server.replication;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import sd2526.trab.api.Message;

/**
 * Represents a replicated operation that can be serialized to/from JSON
 * for Kafka-based state machine replication.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReplicatedOperation {
    private static final ObjectMapper mapper = new ObjectMapper();

    public enum OpType {
        POST_MESSAGE,
        FORWARD_MESSAGE,
        REMOVE_INBOX_MESSAGE,
        DELETE_MESSAGE,
        DELETE_PROPAGATED_MESSAGE,
        SYNC
    }

    private OpType type;
    private String pwd;
    private Message message;
    private String userName;
    private String messageId;

    // Pre-resolved phase-1 data for POST_MESSAGE (avoids calling Users service in
    // phase 2)
    private String resolvedMid;
    private java.util.Set<String> resolvedPersistedDests;
    private java.util.Map<String, java.util.Set<String>> resolvedRemoteForwards;
    private java.util.List<String> resolvedUnknownDests;
    private String resolvedSenderAddress;
    private String resolvedUserAddress;

    // Pre-resolved phase-1 data for FORWARD_MESSAGE
    private java.util.Set<String> resolvedValidForwardDests;
    private java.util.List<String> resolvedUnknownForwardDests;

    public ReplicatedOperation() {
    }

    public ReplicatedOperation(OpType type, String pwd, Message message, String userName, String messageId) {
        this.type = type;
        this.pwd = pwd;
        this.message = message;
        this.userName = userName;
        this.messageId = messageId;
    }

    public static ReplicatedOperation postMessage(String pwd, Message msg) {
        return new ReplicatedOperation(OpType.POST_MESSAGE, pwd, msg, null, null);
    }

    public static ReplicatedOperation postMessage(String pwd, Message msg,
            String mid,
            java.util.Set<String> persistedDests,
            java.util.Map<String, java.util.Set<String>> remoteForwards,
            java.util.List<String> unknownDests,
            String senderAddress) {
        ReplicatedOperation op = new ReplicatedOperation(OpType.POST_MESSAGE, pwd, msg, null, null);
        op.resolvedMid = mid;
        op.resolvedPersistedDests = persistedDests;
        op.resolvedRemoteForwards = remoteForwards;
        op.resolvedUnknownDests = unknownDests;
        op.resolvedSenderAddress = senderAddress;
        return op;
    }

    public static ReplicatedOperation forwardMessage(Message msg) {
        return new ReplicatedOperation(OpType.FORWARD_MESSAGE, null, msg, null, null);
    }

    public static ReplicatedOperation forwardMessage(Message msg, Long sid, String sourceDomain) {
        return new ReplicatedOperation(OpType.FORWARD_MESSAGE, null, msg, null, null);
    }

    public static ReplicatedOperation forwardMessage(Message msg, Long sid, String sourceDomain,
            java.util.Set<String> validLocalDests,
            java.util.List<String> unknownForwardDests) {
        ReplicatedOperation op = new ReplicatedOperation(OpType.FORWARD_MESSAGE, null, msg, null, null);
        op.resolvedValidForwardDests = validLocalDests;
        op.resolvedUnknownForwardDests = unknownForwardDests;
        return op;
    }

    public static ReplicatedOperation removeInboxMessage(String name, String mid, String pwd) {
        return new ReplicatedOperation(OpType.REMOVE_INBOX_MESSAGE, pwd, null, name, mid);
    }

    public static ReplicatedOperation removeInboxMessage(String name, String mid, String pwd, String userAddress) {
        ReplicatedOperation op = new ReplicatedOperation(OpType.REMOVE_INBOX_MESSAGE, pwd, null, name, mid);
        op.resolvedUserAddress = userAddress;
        return op;
    }

    public static ReplicatedOperation deleteMessage(String name, String mid, String pwd) {
        return new ReplicatedOperation(OpType.DELETE_MESSAGE, pwd, null, name, mid);
    }

    public static ReplicatedOperation deleteMessage(String name, String mid, String pwd, String userAddress) {
        ReplicatedOperation op = new ReplicatedOperation(OpType.DELETE_MESSAGE, pwd, null, name, mid);
        op.resolvedUserAddress = userAddress;
        return op;
    }

    public static ReplicatedOperation deletePropagatedMessage(String mid) {
        return new ReplicatedOperation(OpType.DELETE_PROPAGATED_MESSAGE, null, null, null, mid);
    }

    public static ReplicatedOperation sync() {
        return new ReplicatedOperation(OpType.SYNC, null, null, null, null);
    }

    public static ReplicatedOperation deletePropagatedMessage(String mid, Long sid, String sourceDomain) {
        return new ReplicatedOperation(OpType.DELETE_PROPAGATED_MESSAGE, null, null, null, mid);
    }

    public String toJson() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static ReplicatedOperation fromJson(String json) {
        try {
            return mapper.readValue(json, ReplicatedOperation.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Getters and setters
    public OpType getType() {
        return type;
    }

    public void setType(OpType type) {
        this.type = type;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public String getUserName() {
        return userName;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getResolvedMid() {
        return resolvedMid;
    }

    public java.util.Set<String> getResolvedPersistedDests() {
        return resolvedPersistedDests;
    }

    public java.util.Map<String, java.util.Set<String>> getResolvedRemoteForwards() {
        return resolvedRemoteForwards;
    }

    public java.util.List<String> getResolvedUnknownDests() {
        return resolvedUnknownDests;
    }

    public String getResolvedSenderAddress() {
        return resolvedSenderAddress;
    }

    public String getResolvedUserAddress() {
        return resolvedUserAddress;
    }

    public java.util.Set<String> getResolvedValidForwardDests() {
        return resolvedValidForwardDests;
    }

    public java.util.List<String> getResolvedUnknownForwardDests() {
        return resolvedUnknownForwardDests;
    }
}
