package sd2526.trab.api.server.java;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;

import java.util.List;

/**
 * Client abstraction used to send internal message-propagation requests to another domain.
 */
public interface RemoteMessagesClient {
    Result<List<String>> forwardMessage(Message msg);
    Result<Void> deletePropagatedMessage(String mid);
}
