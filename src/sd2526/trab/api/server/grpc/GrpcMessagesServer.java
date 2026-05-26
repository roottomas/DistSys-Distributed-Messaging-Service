package sd2526.trab.api.server.grpc;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import sd2526.trab.api.java.Discovery;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.clients.MessagesClientFactory;
import sd2526.trab.api.clients.UsersClientFactory;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.common.TLSUtils;
import sd2526.trab.api.server.java.JavaMessages;

import java.util.logging.Logger;

public class GrpcMessagesServer {
    private static final Logger Log = Logger.getLogger(GrpcMessagesServer.class.getName());
    public static final int PORT = 9001;

    public static void main(String[] args) {
        ServerSupport.initRuntime();
        Hibernate.resetMessagesInstance();
        try {
            long extraLongFaultMillis = ServerSupport.parseExtraLongFaultMillis(args, 90_000);
            String secret = ServerSupport.parseSecret(args);
            if (secret != null)
                ServerSupport.setServerSecret(secret);

            String host = ServerSupport.hostName();
            String serverURI = ServerSupport.grpcUri(host, PORT);

            Discovery discovery = ServerSupport.startDiscovery(
                    Messages.SERVICE_NAME,
                    ServerSupport.localDomain(),
                    serverURI);

            var usersFactory = new UsersClientFactory(discovery);
            JavaMessages impl = new JavaMessages(
                    Hibernate.getMessagesInstance(),
                    ServerSupport::localDomain,
                    usersFactory.get(ServerSupport.localDomain()),
                    new MessagesClientFactory(discovery),
                    extraLongFaultMillis);

            SslContext sslContext = TLSUtils.getGrpcServerSslContext();
            Server server = NettyServerBuilder.forPort(PORT)
                    .sslContext(sslContext)
                    .intercept(new SecretInterceptor())
                    .addService(new GrpcMessagesController(impl))
                    .build();

            server.start();
            Log.info("%s Server ready @ %s".formatted(Messages.SERVICE_NAME, serverURI));
            server.awaitTermination();
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }
}
