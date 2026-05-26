package sd2526.trab.api.java;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service discovery based on periodic multicast announcements.
 *
 * Announcement format:
 * <service-name>@<domain>\t<service-uri>
 */
public class Discovery {
    private static Logger Log = Logger.getLogger(Discovery.class.getName());

    private final Map<String, Map<URI, Long>> services = new ConcurrentHashMap<>();

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
    }

    public static final InetSocketAddress DISCOVERY_ADDR =
            new InetSocketAddress("226.226.226.226", 2266);

    public static final int DISCOVERY_ANNOUNCE_PERIOD = 1000;
    public static final int DISCOVERY_TIMEOUT = 5000;
    public static final int MAX_DATAGRAM_SIZE = 65536;

    private static final String DELIMITER = "\t";

    private final InetSocketAddress addr;
    private final String serviceName;
    private final String serviceURI;
    private final MulticastSocket ms;

    public Discovery(InetSocketAddress addr, String serviceName, String serviceURI)
            throws IOException {
        this.addr = addr;
        this.serviceName = serviceName;
        this.serviceURI = serviceURI;

        if (this.addr == null)
            throw new RuntimeException("A multicast address has to be provided.");

        this.ms = new MulticastSocket(addr.getPort());
        this.ms.joinGroup(addr, NetworkInterface.getByInetAddress(InetAddress.getLocalHost()));
    }

    public Discovery(InetSocketAddress addr)
            throws IOException {
        this(addr, null, null);
    }

    /**
     * Starts the discovery service.
     * If this instance has a local service name and URI configured, it starts a
     * background thread that periodically announces that service through multicast.
     * It also starts a background listener thread that continuously receives
     * multicast announcements from other services and records their URIs together
     * with the time they were last seen.
     */
    public void start() {
        if (this.serviceName != null && this.serviceURI != null) {
            Log.info(String.format("Starting Discovery announcements on: %s for: %s -> %s",
                    addr, serviceName, serviceURI));

            final byte[] announceBytes =
                    String.format("%s%s%s", serviceName, DELIMITER, serviceURI).getBytes();
            final DatagramPacket announcePkt =
                    new DatagramPacket(announceBytes, announceBytes.length, addr);

            final ScheduledExecutorService scheduler =
                    Executors.newSingleThreadScheduledExecutor();

            scheduler.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        ms.send(announcePkt);
                    } catch (Exception e) {
                        Log.log(Level.WARNING, "Error sending discovery announcement.", e);
                    }
                }
            }, 0, DISCOVERY_ANNOUNCE_PERIOD, TimeUnit.MILLISECONDS);
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                DatagramPacket pkt =
                        new DatagramPacket(new byte[MAX_DATAGRAM_SIZE], MAX_DATAGRAM_SIZE);

                for (;;) {
                    try {
                        pkt.setLength(MAX_DATAGRAM_SIZE);
                        ms.receive(pkt);

                        String msg = new String(pkt.getData(), 0, pkt.getLength());
                        String[] elems = msg.split(DELIMITER);

                        if (elems.length == 2) {
                            String name = elems[0];
                            URI uri = URI.create(elems[1]);

                            services.putIfAbsent(name, new ConcurrentHashMap<URI, Long>());
                            services.get(name).put(uri, System.currentTimeMillis());

                            Log.info(String.format("Discovered: %s -> %s", name, uri));
                        }
                    } catch (IOException e) {
                        // ignore and keep listening
                    }
                }
            }
        }).start();
    }

    /**
     * Returns the currently known URIs for a given service name.
     * Before returning, expired service announcements are removed, according to the
     * discovery timeout. The method blocks until at least {@code minReplies}
     * non-expired URIs are known for the requested service.
     * @param serviceName the name of the service to look up.
     * @param minReplies the minimum number of known URIs required before returning.
     * @return an array containing the known non-expired URIs of the requested service.
     */
    public URI[] knownUrisOf(String serviceName, int minReplies) {
        while (true) {
            Map<URI, Long> uris = services.get(serviceName);

            if (uris != null) {
                long now = System.currentTimeMillis();

                uris.entrySet().removeIf(entry -> now - entry.getValue() > DISCOVERY_TIMEOUT);

                if (uris.size() >= minReplies)
                    return uris.keySet().toArray(new URI[0]);
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException ignored) {
            }
        }
    }
}
