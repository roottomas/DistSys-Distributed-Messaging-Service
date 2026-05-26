package sd2526.trab.api.rest;

import java.util.List;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.User;

/**
 * REST contract for the users service.
 *
 * <p>This interface exposes the user-management operations offered by each
 * domain, plus an internal lookup used by the messages service when validating
 * recipients inside the same domain.
 */
@Path(RestUsers.PATH)
public interface RestUsers {

	/** Base REST path of the users resource. */
	final String PATH = "/users";
	/** Query parameter used for user searches. */
	final String QUERY = "query";
	/** Path/query parameter that identifies a user name. */
	final String NAME = "name";
	/** Query parameter used to transport a password. */
	final String PWD = "pwd";
	
	/**
	 * Creates a new local user.
	 *
	 * @param user user to create
	 * @return canonical address of the created user
	 */
	@POST
	@Path("/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	String postUser(User user);
	
	/**
	 * Retrieves the authenticated user's public data.
	 *
	 * @param name user name
	 * @param pwd user's password
	 * @return stored user
	 */
	@GET
	@Path("/{" + NAME +"}")
	@Produces(MediaType.APPLICATION_JSON)
	User getUser(@PathParam(NAME) String name, @QueryParam(PWD) String pwd);
	
	/**
	 * Updates mutable information of an authenticated user.
	 *
	 * @param name user name
	 * @param pwd user's password
	 * @param info partial user object with the requested changes
	 * @return updated user
	 */
	@PUT
	@Path("/{" + NAME +"}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	User updateUser(@PathParam(NAME) String name, @QueryParam(PWD) String pwd, User info);
	
	/**
	 * Deletes an authenticated user.
	 *
	 * @param name user name
	 * @param pwd user's password
	 * @return deleted user
	 */
	@DELETE
	@Path("/{" + NAME +"}")
	@Produces(MediaType.APPLICATION_JSON)
	User deleteUser(@PathParam(NAME) String name, @QueryParam(PWD) String pwd);
	
	/**
	 * Retrieves a user without password validation.
	 *
	 * <p>This endpoint is intended for internal service-to-service calls inside
	 * the same domain.
	 *
	 * @param name user name
	 * @return stored user
	 */
	@GET
	@Path("/internal/{" + NAME +"}")
	@Produces(MediaType.APPLICATION_JSON)
	User getInternalUser(@PathParam(NAME) String name);
	
	/**
	 * Searches users visible to the authenticated caller.
	 *
	 * @param name user performing the search
	 * @param pwd caller's password
	 * @param pattern case-insensitive substring to match
	 * @return matching users
	 */
	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	List<User> searchUsers(@QueryParam(NAME) String name, @QueryParam(PWD) String pwd, @QueryParam(QUERY) String pattern);	
}
