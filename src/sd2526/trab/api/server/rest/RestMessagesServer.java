package sd2526.trab.api.server.rest;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Discovery;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.common.TLSUtils;

import java.net.URI;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;

public class RestMessagesServer {
    private static final Logger Log = Logger.getLogger(RestMessagesServer.class.getName());
    public static Discovery discovery;
    public static long EXTRA_LONG_FAULT_MILLIS = 90_000;
    public static final int PORT = 8081;

    public static void main(String[] args) {
        ServerSupport.initRuntime();
        Hibernate.resetMessagesInstance();
        try {
            EXTRA_LONG_FAULT_MILLIS = ServerSupport.parseExtraLongFaultMillis(args, 90_000);
            String secret = ServerSupport.parseSecret(args);
            if (secret != null)
                ServerSupport.setServerSecret(secret);

            String host = ServerSupport.hostName();
            String serverURI = ServerSupport.restUri(host, PORT);
            discovery = ServerSupport.startDiscovery(Messages.SERVICE_NAME, ServerSupport.localDomain(), serverURI);
            ResourceConfig config = new ResourceConfig().register(RestMessagesResource.class);

            SSLContext sslContext = TLSUtils.getServerSSLContext();
            JdkHttpServerFactory.createHttpServer(URI.create(serverURI), config, sslContext);

            Log.info("%s Server ready @ %s".formatted(Messages.SERVICE_NAME, serverURI));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
