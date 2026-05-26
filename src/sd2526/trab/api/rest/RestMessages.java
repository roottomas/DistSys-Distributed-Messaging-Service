package sd2526.trab.api.rest;

import java.util.List;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;

/**
 * REST contract for the messages service.
 *
 * <p>
 * This interface defines the public mailbox operations exposed to clients
 * and the internal operations used by other message servers when propagating
 * deliveries and deletions across domains.
 */
@Path(RestMessages.PATH)
public interface RestMessages {

	final String PATH = "/messages";
	final String QUERY = "query";
	final String NAME = "name";
	final String PWD = "pwd";
	final String MID = "mid";
	final String MBOX = "/mbox";
	final String SECRET = "X-SECRET";

	/**
	 * Creates a new outgoing message on behalf of an authenticated user.
	 *
	 * @param pwd password of the sender
	 * @param msg message payload to create and distribute
	 * @return identifier assigned to the created message
	 */
	@POST
	@Path("/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	String postMessage(@QueryParam(PWD) String pwd, Message msg);

	/**
	 * Accepts a message that was already created in another domain and stores the
	 * local deliveries that belong to this domain.
	 *
	 * @param msg message received from a remote domain
	 * @return list of local destinations that do not exist in this domain
	 */
	@POST
	@Path("/internal")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	List<String> forwardMessage(Message msg);

	/**
	 * Removes a propagated message copy from this domain without performing sender
	 * authentication again.
	 *
	 * @param mid identifier of the message to remove locally
	 */
	@DELETE
	@Path("/internal/{" + MID + "}")
	void deletePropagatedMessage(@PathParam(MID) String mid);

	/**
	 * Retrieves one message from the authenticated user's inbox.
	 *
	 * @param name inbox owner
	 * @param mid  message identifier
	 * @param pwd  password of the inbox owner
	 * @return stored message
	 */
	@GET
	@Path(MBOX + "/{" + NAME + "}/{" + MID + "}")
	@Produces(MediaType.APPLICATION_JSON)
	Message getMessage(@PathParam(NAME) String name, @PathParam(MID) String mid, @QueryParam(PWD) String pwd);

	/**
	 * Lists the ids of messages in a user's inbox.
	 *
	 * <p>
	 * If {@code query} is blank, all inbox message ids are returned. Otherwise
	 * only messages whose subject or contents contain the query, ignoring case,
	 * are returned.
	 *
	 * @param name  inbox owner
	 * @param pwd   password of the inbox owner
	 * @param query optional mailbox search text
	 * @return matching inbox message ids
	 */
	@GET
	@Path(MBOX + "/{" + NAME + "}")
	@Produces(MediaType.APPLICATION_JSON)
	List<String> getMessages(@PathParam(NAME) String name, @QueryParam(PWD) String pwd,
			@QueryParam(QUERY) @DefaultValue("") String query);

	/**
	 * Removes one message from a user's inbox.
	 *
	 * @param name inbox owner
	 * @param mid  message identifier
	 * @param pwd  password of the inbox owner
	 */
	@DELETE
	@Path(MBOX + "/{" + NAME + "}/{" + MID + "}")
	void removeFromUserInbox(@PathParam(NAME) String name, @PathParam(MID) String mid, @QueryParam(PWD) String pwd);

	/**
	 * Deletes a recently sent message from all places where it is still stored.
	 *
	 * @param name original sender
	 * @param mid  message identifier
	 * @param pwd  password of the sender
	 */
	@DELETE
	@Path("/{" + NAME + "}/{" + MID + "}")
	void deleteMessage(@PathParam(NAME) String name, @PathParam(MID) String mid, @QueryParam(PWD) String pwd);
}
