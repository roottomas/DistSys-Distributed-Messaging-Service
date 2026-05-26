package sd2526.trab.api.server.rest;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Discovery;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.common.TLSUtils;

import java.net.URI;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;

public class RestUsersServer {
    private static final Logger Log = Logger.getLogger(RestUsersServer.class.getName());
    public static Discovery discovery;
    public static final int PORT = 8080;

    public static void main(String[] args) {
        ServerSupport.initRuntime();
        Hibernate.resetUsersInstance();
        try {
            String secret = ServerSupport.parseSecret(args);
            if (secret != null)
                ServerSupport.setServerSecret(secret);

            ResourceConfig config = new ResourceConfig().register(RestUsersResource.class);
            String host = ServerSupport.hostName();
            String serverURI = ServerSupport.restUri(host, PORT);

            SSLContext sslContext = TLSUtils.getServerSSLContext();
            JdkHttpServerFactory.createHttpServer(URI.create(serverURI), config, sslContext);

            Log.info("%s Server ready @ %s".formatted(Users.SERVICE_NAME, serverURI));
            discovery = ServerSupport.startDiscovery(Users.SERVICE_NAME, ServerSupport.localDomain(), serverURI);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
