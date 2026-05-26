package sd2526.trab.api.java;

import java.io.File;
import java.net.InetAddress;
import java.util.List;

import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

/**
 * Helper class for POJO persistence using Hibernate.
 * Uses one Hibernate helper per server process, backed by a DB file that is
 * already isolated by container hostname.
 */
public class Hibernate {
    private static final String HIBERNATE_CFG_FILE = "hibernate.cfg.xml";

    private static volatile Hibernate usersInstance;
    private static volatile Hibernate messagesInstance;

    private final SessionFactory sessionFactory;

    /**
     * Creates a Hibernate helper bound to one database URL and a set of annotated
     * entity classes.
     *
     * @param dbUrl            JDBC URL of the backing database
     * @param annotatedClasses entity classes managed by this helper
     */
    private Hibernate(String dbUrl, Class<?>... annotatedClasses) {
        try {
            Configuration config = new Configuration()
                    .configure(new File(HIBERNATE_CFG_FILE));

            if (dbUrl != null)
                config.setProperty("hibernate.connection.url", dbUrl);

            config.setProperty("hibernate.connection.pool_size", "100");

            for (Class<?> clazz : annotatedClasses)
                config.addAnnotatedClass(clazz);

            sessionFactory = config.buildSessionFactory();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds a filesystem-safe suffix derived from the current host name.
     *
     * @return sanitized host suffix
     */
    private static String hostSuffix() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            return sanitizeHost(host);
        } catch (Exception e) {
            return "unknown";
        }
    }

    /**
     * Normalizes a host name so it can be embedded in database file names.
     *
     * @param host raw host name
     * @return result of the operation.
     */
    private static String sanitizeHost(String host) {
        return host == null || host.isBlank() ? "unknown" : host.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    /**
     * Returns the singleton helper used by the users service.
     *
     * @return users persistence helper
     */
    public static Hibernate getUsersInstance() {
        Hibernate h = usersInstance;
        if (h == null) {
            synchronized (Hibernate.class) {
                h = usersInstance;
                if (h == null) {
                    h = new Hibernate("jdbc:hsqldb:mem:usersdb-" + hostSuffix(), sd2526.trab.api.User.class);
                    usersInstance = h;
                }
            }
        }
        return h;
    }

    /**
     * Returns the singleton helper used by the messages service.
     *
     * @return messages persistence helper
     */
    public static Hibernate getMessagesInstance() {
        Hibernate h = messagesInstance;
        if (h == null) {
            synchronized (Hibernate.class) {
                h = messagesInstance;
                if (h == null) {
                    h = new Hibernate("jdbc:hsqldb:mem:messagesdb-" + hostSuffix(), sd2526.trab.api.Message.class);
                    messagesInstance = h;
                }
            }
        }
        return h;
    }

    public static synchronized void resetUsersInstance() {
        Hibernate h = usersInstance;
        usersInstance = null;
        if (h != null)
            h.closeQuietly();
    }

    public static synchronized void resetMessagesInstance() {
        Hibernate h = messagesInstance;
        messagesInstance = null;
        if (h != null)
            h.closeQuietly();
    }

    private void closeQuietly() {
        try {
            sessionFactory.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Persists the operation.
     *
     * @param objects entities to persist or delete.
     */
    public void persist(Object... objects) {
        var session = sessionFactory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            for (var o : objects)
                session.persist(o);
            tx.commit();
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception ignored) {
                }
            }
            throw e;
        } finally {
            session.close();
        }
    }

    /**
     * Retrieves one entity by primary key.
     *
     * @param clazz      entity class to query.
     * @param identifier primary-key value of the entity to retrieve.
     * @return result of the operation.
     */
    public <T> T get(Class<T> clazz, Object identifier) {
        var session = sessionFactory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            T element = session.get(clazz, identifier);
            tx.commit();
            return element;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception ignored) {
                }
            }
            throw e;
        } finally {
            session.close();
        }
    }

    /**
     * Updates the operation.
     *
     * @param objects entities to persist or delete.
     */
    public void update(Object... objects) {
        var session = sessionFactory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            for (var o : objects)
                session.merge(o);
            tx.commit();
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception ignored) {
                }
            }
            throw e;
        } finally {
            session.close();
        }
    }

    /**
     * Deletes the supplied entities.
     *
     * @param objects entities to persist or delete.
     */
    public void delete(Object... objects) {
        var session = sessionFactory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            for (var o : objects)
                session.remove(session.contains(o) ? o : session.merge(o));
            tx.commit();
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception ignored) {
                }
            }
            throw e;
        } finally {
            session.close();
        }
    }

    /**
     * Executes a JPQL query and returns its results.
     *
     * @param jpqlStatement JPQL statement to execute.
     * @param clazz         entity class to query.
     * @return result of the operation.
     */
    public <T> List<T> jpql(String jpqlStatement, Class<T> clazz) {
        var session = sessionFactory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            var query = session.createQuery(jpqlStatement, clazz);
            List<T> result = query.list();
            tx.commit();
            return result;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception ignored) {
                }
            }
            throw e;
        } finally {
            session.close();
        }
    }

    /**
     * Executes a native SQL query and returns its results.
     *
     * @param sqlStatement native SQL statement to execute.
     * @param clazz        entity class to query.
     * @return result of the operation.
     */
    public <T> List<T> sql(String sqlStatement, Class<T> clazz) {
        var session = sessionFactory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            var query = session.createNativeQuery(sqlStatement, clazz);
            List<T> result = query.list();
            tx.commit();
            return result;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception ignored) {
                }
            }
            throw e;
        } finally {
            session.close();
        }
    }
}
