package sd2526.trab.api.server.grpc;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import sd2526.trab.api.java.Discovery;
import sd2526.trab.api.java.Hibernate;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.server.common.ServerSupport;
import sd2526.trab.api.server.common.TLSUtils;

import java.util.logging.Logger;

public class GrpcUsersServer {
    private static final Logger Log = Logger.getLogger(GrpcUsersServer.class.getName());
    public static final int PORT = 9000;
    public static Discovery discovery;

    public static void main(String[] args) {
        ServerSupport.initRuntime();
        Hibernate.resetUsersInstance();
        try {
            String secret = ServerSupport.parseSecret(args);
            if (secret != null)
                ServerSupport.setServerSecret(secret);

            String host = ServerSupport.hostName();
            String serverURI = ServerSupport.grpcUri(host, PORT);

            SslContext sslContext = TLSUtils.getGrpcServerSslContext();
            Server server = NettyServerBuilder.forPort(PORT)
                    .sslContext(sslContext)
                    .addService(new GrpcUsersController())
                    .build();

            server.start();
            Log.info("%s Server ready @ %s".formatted(Users.SERVICE_NAME, serverURI));
            discovery = ServerSupport.startDiscovery(Users.SERVICE_NAME, ServerSupport.localDomain(), serverURI);
            server.awaitTermination();
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }
}
