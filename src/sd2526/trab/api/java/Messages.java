package sd2526.trab.api.java;

import java.util.List;

import sd2526.trab.api.Message;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

/**
 * Service contract implemented by the messages service.
 *
 * <p>
 * This interface is shared by the local Java implementation and by the REST
 * and gRPC adapters so that all transports expose the same semantics.
 */
@Path(Messages.PATH)
public interface Messages {

	final static String PATH = "/messages";
	final static String MID = "mid";
	final static String PWD = "pwd";
	final static String NAME = "name";
	String SERVICE_NAME = "Messages";

	/**
	 * Posts a new message to the server, associating it to the inbox of each
	 * individual destination.
	 * An outgoing message should be modified before delivering it, by assigning it
	 * an ID, and by changing the
	 * sender to be in the format "display name <name@domain>".
	 * 
	 * Posting exactly the same message more than once, should have no effect but
	 * still succeed by returning its ID.
	 * 
	 * NOTE: there might be some destinations that are not from the local domain
	 * (see grading for
	 * how addressing this feature is valued).
	 * 
	 * @param msg the message object to be posted to the server
	 * @param pwd password of the user posting the message
	 * 
	 * @return (OK, the unique identifier for the posted message)
	 *         FORBIDDEN - if the sender does not exist, or, if the pwd is not
	 *         correct (NOTE: sender can be in the form
	 *         "name" or "name@domain");
	 *         BAD_REQUEST if the parameters are invalid (eg., null parameters, or a
	 *         malformed message).
	 */
	@POST
	@Path("/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	Result<String> postMessage(@QueryParam(PWD) String pwd, Message msg);

	/**
	 * Obtains a single message from the inbox of a user.
	 * 
	 * @param name the owner of the inbox
	 * @param mid  the identifier of the message to be retrieved
	 * @param pwd  password of the owner of the inbox
	 * @return (OK, the message if it exists);
	 *         FORBIDEN - if the name does not exist or if the password is not
	 *         correct;
	 *         NOT_FOUND - if the message does not exist;
	 *         BAD_REQUEST if the parameters are invalid (eg., null parameters).
	 */
	@GET
	@Path("/{" + NAME + "}/{" + MID + "}")
	@Produces(MediaType.APPLICATION_JSON)
	Result<Message> getInboxMessage(@PathParam(NAME) String name, @PathParam(MID) String mid,
			@QueryParam(PWD) String pwd);

	/**
	 * Returns all the messages from the inbox of a user.
	 * 
	 * @param name the owner of the inbox
	 * @param pwd  password of the owner of the inbox
	 * @return (OK, list of ids, potentially empty);
	 *         FORBIDDEN if the name does not exist or if the password is not
	 *         correct.
	 *         BAD_REQUEST if the parameters are invalid (eg., null parameters).
	 */
	@GET
	@Path("/{" + NAME + "}")
	@Produces(MediaType.APPLICATION_JSON)
	Result<List<String>> getAllInboxMessages(@PathParam(NAME) String name, @QueryParam(PWD) String pwd);

	/**
	 * Removes a single message from the inbox of a user.
	 * 
	 * @param name the owner of the inbox
	 * @param mid  the identifier of the message to be deleted
	 * @param pwd  password of the owner of the inbox
	 * @return NO_CONTENT if ok
	 *         FORBIDDEN if the name does not exist or if the password is not
	 *         correct.
	 *         NOT_FOUND - if the message does not exist;
	 *         BAD_REQUEST if the parameters are invalid (eg., null parameters).
	 */
	@DELETE
	@Path("/{" + NAME + "}/{" + MID + "}")
	@Produces(MediaType.APPLICATION_JSON)
	Result<Void> removeInboxMessage(@PathParam(NAME) String name, @PathParam(MID) String mid,
			@QueryParam(PWD) String pwd);

	/**
	 * Deletes a message from all inboxes and servers where it is stored. A message
	 * should only be deleted if it was
	 * posted less than 30 seconds ago.
	 * 
	 * The deletion can be executed asynchronously and does not generate an error
	 * message if the
	 * message does not exist.
	 * 
	 * @param name the sender of the message to be deleted
	 * @param mid  the identifier of the message to be deleted
	 * @param pwd  password of the sender
	 * @return NO_CONTENT if ok
	 *         FORBIDDEN if the sender does not exist or if the password is not
	 *         correct;
	 *         BAD_REQUEST if the parameters are invalid (eg., null parameters).
	 */
	@DELETE
	@Path("/{" + NAME + "}")
	@Produces(MediaType.APPLICATION_JSON)
	Result<Void> deleteMessage(@PathParam(NAME) String name, @QueryParam(MID) String mid, @QueryParam(PWD) String pwd);

	/**
	 * Returns the list of identifiers of the messages in the inbox of a user that
	 * match a query. A message matches
	 * when the query is a substring of the subject or contents, case-insensitive.
	 * 
	 * @param name  the owner of the inbox
	 * @param pwd   password of the owner of the inbox
	 * @param query substring to search for in subject or contents
	 * @return (OK, list of hits, may be empty.) .
	 *         FORBIDDEN if the name does not exist or if the password is not
	 *         correct.
	 *         BAD_REQUEST if the parameters are invalid (eg., null parameters).
	 */
	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	Result<List<String>> searchInbox(@QueryParam(NAME) String name, @QueryParam(PWD) String pwd,
			@QueryParam("query") String query);

	/**
	 * Receives a message forwarded from a remote domain and stores it for local
	 * recipients.
	 *
	 * @param msg forwarded message
	 * @return list of unknown local recipients, or an error result
	 */
	@POST
	@Path("/internal")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	Result<List<String>> forwardMessage(Message msg);

	/**
	 * Removes a propagated message copy from this domain.
	 * Called by a remote domain when the original sender deletes a message.
	 *
	 * @param mid message id to delete
	 * @return OK on success, or an error result
	 */
	@DELETE
	@Path("/internal/{" + MID + "}")
	Result<Void> deletePropagatedMessage(@PathParam(MID) String mid);

}
