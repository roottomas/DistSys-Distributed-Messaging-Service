package sd2526.trab.api.server.rest;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Discovery;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.clients.MessagesClientFactory;
import sd2526.trab.api.clients.UsersClientFactory;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.common.TLSUtils;
import sd2526.trab.api.server.replication.ReplicatedMessages;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.util.logging.Logger;

/**
 * Bootstrap class for the replicated REST messages server using Kafka.
 */
public class RestReplicatedMessagesServer {
    private static final Logger Log = Logger.getLogger(RestReplicatedMessagesServer.class.getName());
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
            String domain = ServerSupport.localDomain();

            discovery = ServerSupport.startDiscovery(Messages.SERVICE_NAME, domain, serverURI);

            // Kafka topic is the domain name - all replicas of the same domain use the same
            // topic
            String kafkaTopic = "messages-" + domain;

            UsersClientFactory usersFactory = new UsersClientFactory(discovery);
            MessagesClientFactory messagesFactory = new MessagesClientFactory(discovery);

            ReplicatedMessages replicatedImpl = new ReplicatedMessages(
                    Hibernate.getMessagesInstance(),
                    ServerSupport::localDomain,
                    usersFactory.get(domain),
                    messagesFactory,
                    EXTRA_LONG_FAULT_MILLIS,
                    kafkaTopic);

            ResourceConfig config = new ResourceConfig()
                    .registerInstances(new RestReplicatedMessagesResource(replicatedImpl))
                    .register(new VersionResponseFilter(replicatedImpl));

            SSLContext sslContext = TLSUtils.getServerSSLContext();
            JdkHttpServerFactory.createHttpServer(URI.create(serverURI), config, sslContext);

            Log.info("%s Replicated Server ready @ %s (topic: %s)".formatted(
                    Messages.SERVICE_NAME, serverURI, kafkaTopic));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
