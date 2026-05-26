package sd2526.trab.api.server.rest;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Discovery;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.common.TLSUtils;
import sd2526.trab.api.server.proxy.ProxyMessages;
import sd2526.trab.api.server.proxy.ZohoMailClient;

import java.net.URI;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;

/**
 * Bootstrap class for the Proxy Messages REST server.
 *
 * <p>Creates a {@link ProxyMessages} instance backed by the configured Zoho Mail
 * account, registers it with a {@link RestProxyMessagesResource} JAX-RS adapter,
 * and starts an HTTPS server using the JDK HTTP server factory.  The server
 * announces its address on the multicast discovery channel so that peer servers
 * can locate it.
 */
public class RestProxyMessagesServer {
    private static final Logger Log = Logger.getLogger(RestProxyMessagesServer.class.getName());
    public static final int PORT = 8081;

    // Zoho OAuth credentials - must be configured for deployment
    private static final String ZOHO_CLIENT_ID = "1000.T1B43SAVRE6Y6XM660ZIAE7HUU5O2R";
    private static final String ZOHO_CLIENT_SECRET = "6a3ca5d687b84a57d7b3925706b84f670ebf61ae28";
    private static final String ZOHO_REFRESH_TOKEN = "1000.0328df2f8b5a7a022fe8ea790cc8fdc5.edcd7152d1c294709e174103456f3f87";

    public static void main(String[] args) {
        ServerSupport.initRuntime();
        try {
            // First arg: true/false for clean state
            boolean cleanState = args.length > 0 && "true".equalsIgnoreCase(args[0]);

            // Parse secret from remaining args
            String secret = ServerSupport.parseSecret(args);
            if (secret != null)
                ServerSupport.setServerSecret(secret);

            String host = ServerSupport.hostName();
            String serverURI = ServerSupport.restUri(host, PORT);

            Discovery discovery = ServerSupport.startDiscovery(Messages.SERVICE_NAME, ServerSupport.localDomain(),
                    serverURI);

            // Initialize Zoho client and proxy messages
            ZohoMailClient zoho = new ZohoMailClient(ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN);
            ProxyMessages proxyMessages = new ProxyMessages(zoho, discovery, cleanState);

            ResourceConfig config = new ResourceConfig()
                    .registerInstances(new RestProxyMessagesResource(proxyMessages));

            SSLContext sslContext = TLSUtils.getServerSSLContext();
            JdkHttpServerFactory.createHttpServer(URI.create(serverURI), config, sslContext);

            Log.info("%s Proxy Server ready @ %s (cleanState=%s)".formatted(Messages.SERVICE_NAME, serverURI,
                    cleanState));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
