package sd2526.trab.api.server.common;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.logging.Logger;

/**
 * Utility class for TLS/SSL configuration shared by all servers and clients.
 */
public final class TLSUtils {
    private static final Logger Log = Logger.getLogger(TLSUtils.class.getName());

    private TLSUtils() {}

    /**
     * Creates an SSLContext for REST servers using the keystore and truststore
     * configured via system properties.
     */
    public static SSLContext getServerSSLContext() {
        try {
            String keyStoreFile = System.getProperty("javax.net.ssl.keyStore");
            String keyStorePwd = System.getProperty("javax.net.ssl.keyStorePassword");
            String trustStoreFile = System.getProperty("javax.net.ssl.trustStore");
            String trustStorePwd = System.getProperty("javax.net.ssl.trustStorePassword");

            KeyManagerFactory kmf = null;
            if (keyStoreFile != null) {
                KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
                try (FileInputStream fis = new FileInputStream(keyStoreFile)) {
                    ks.load(fis, keyStorePwd != null ? keyStorePwd.toCharArray() : null);
                }
                kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, keyStorePwd != null ? keyStorePwd.toCharArray() : null);
            }

            TrustManagerFactory tmf = null;
            if (trustStoreFile != null) {
                KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
                try (FileInputStream fis = new FileInputStream(trustStoreFile)) {
                    ts.load(fis, trustStorePwd != null ? trustStorePwd.toCharArray() : null);
                }
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
            }

            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(
                    kmf != null ? kmf.getKeyManagers() : null,
                    tmf != null ? tmf.getTrustManagers() : null,
                    null
            );
            return ctx;
        } catch (Exception e) {
            Log.severe("Failed to create server SSLContext: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates an SSLContext for REST clients using only the truststore.
     */
    public static SSLContext getClientSSLContext() {
        try {
            String trustStoreFile = System.getProperty("javax.net.ssl.trustStore");
            String trustStorePwd = System.getProperty("javax.net.ssl.trustStorePassword");

            TrustManagerFactory tmf = null;
            if (trustStoreFile != null) {
                KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
                try (FileInputStream fis = new FileInputStream(trustStoreFile)) {
                    ts.load(fis, trustStorePwd != null ? trustStorePwd.toCharArray() : null);
                }
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
            }

            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tmf != null ? tmf.getTrustManagers() : null, null);
            return ctx;
        } catch (Exception e) {
            Log.severe("Failed to create client SSLContext: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a Netty SslContext for gRPC servers using the keystore.
     */
    public static SslContext getGrpcServerSslContext() {
        try {
            String keyStoreFile = System.getProperty("javax.net.ssl.keyStore");
            String keyStorePwd = System.getProperty("javax.net.ssl.keyStorePassword");

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            try (FileInputStream fis = new FileInputStream(keyStoreFile)) {
                ks.load(fis, keyStorePwd != null ? keyStorePwd.toCharArray() : null);
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keyStorePwd != null ? keyStorePwd.toCharArray() : null);

            return GrpcSslContexts.configure(
                    SslContextBuilder.forServer(kmf)
            ).build();
        } catch (Exception e) {
            Log.severe("Failed to create gRPC server SslContext: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a Netty SslContext for gRPC clients using the truststore.
     */
    public static SslContext getGrpcClientSslContext() {
        try {
            String trustStoreFile = System.getProperty("javax.net.ssl.trustStore");
            String trustStorePwd = System.getProperty("javax.net.ssl.trustStorePassword");

            KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
            try (FileInputStream fis = new FileInputStream(trustStoreFile)) {
                ts.load(fis, trustStorePwd != null ? trustStorePwd.toCharArray() : null);
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);

            return GrpcSslContexts.configure(
                    SslContextBuilder.forClient().trustManager(tmf)
            ).build();
        } catch (Exception e) {
            Log.severe("Failed to create gRPC client SslContext: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
