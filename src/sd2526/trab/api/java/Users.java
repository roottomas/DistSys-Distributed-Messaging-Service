package sd2526.trab.api.java;

import java.util.List;

import sd2526.trab.api.User;
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

/**
 * Service contract implemented by the users service.
 *
 * <p>This interface captures the domain operations that all users-service
 * transports must expose.
 */
@Path(Users.PATH)
public interface Users {
	final static String PATH = "/users";
	final static String QUERY = "query";
	final static String NAME = "name";
	final static String PWD = "pwd";
	String SERVICE_NAME = "Users";
	
	/**
	 * Creates a new user in the local domain.
	 * 
	 * The operation succeeds when the name is not already in use; or, when the same user
	 * is already present in the system.
	 *  
	 * @param user - User to be created
	 * @return (OK, the address of the user: name@domain)
	 * FORBIDDEN if the domain in the user does not match the domain of the server;
	 * CONFLICT if the name is already associated with a different user (i.e., password or display name differ);
	 * BAD_REQUEST if the parameters are invalid (eg., null parameters, or a malformed user).
	 */
	@POST
	@Path("/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	Result<String> postUser(User user);
	
	/**
	 * Obtains the information on the user identified by name
	 * 
	 * @param name - the name of the user
	 * @param pwd - password of the user
	 * 
	 * @return (OK, the user object, if the name exists and pwd matches the existing password)
	 * FORBIDDEN if the password is incorrect or the user does not exist; 
	 * BAD_REQUEST if the parameters are invalid (eg., null parameters).
	 */
	@GET
	@Path("/{" + NAME +"}")
	@Produces(MediaType.APPLICATION_JSON)
	Result<User> getUser(@PathParam(NAME) String name, @QueryParam(PWD) String pwd);
	
	/**
	 * Modifies the information of a user. Any field of the provided info containing null should 
	 * be left unchanged (the name cannot be modified).
	 * 
	 * @param name - the name of the user
	 * @param pwd - password of the user
	 * @param info - Updated information
	 * @return (OK, the updated user object, if the name exists and pwd matches the existing password);
	 * FORBIDDEN if the password is incorrect or the user does not exist; 
	 * BAD_REQUEST if the parameters are invalid (eg., null parameters).
	 */
	@PUT
	@Path("/{" + NAME +"}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	Result<User> updateUser(@PathParam(NAME) String name, @QueryParam(PWD) String pwd, User info);
	
	/**
	 * Deletes the user identified by name. 
	 * All resources owned by this user in other services should also be eventually released, without waiting for completion.
	 * @param name - the name of the user
	 * @param pwd - password of the user
	 * @return (OK, the deleted user object, if the name exists and pwd matches the existing password) 
	 * FORBIDDEN if the password is incorrect or the user does not exist 
	 * BAD_REQUEST if the parameters are invalid (eg., null parameters).
	 */
	@DELETE
	@Path("/{" + NAME + "}")
	@Produces(MediaType.APPLICATION_JSON)
	Result<User>  deleteUser(@PathParam(NAME) String name, @QueryParam(PWD) String pwd);
	
	/**
	 * Returns the list of users for which the pattern is a substring of the name, case-insensitive. 
	 * The password of the users returned by the query must be set to the empty string "".
	 * Only existing users can perform a search.
	 * 
	 * @param name - the name of the user performing the search
	 * @param pwd - password of the user
	 * @param query - substring to search
	 * @return (OK, list of hits, may be empty.) . 
	 * FORBIDDEN if the password is incorrect or the user does not exist 
	 * BAD_REQUEST if the parameters are invalid (eg., null parameters).
	 */
	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	Result<List<User>> searchUsers(@QueryParam(NAME) String name, @QueryParam(PWD) String pwd, @QueryParam(QUERY) String query);
	
}
