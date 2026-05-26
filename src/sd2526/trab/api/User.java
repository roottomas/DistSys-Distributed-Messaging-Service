package sd2526.trab.api;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

/**
 * Persistent representation of a user account hosted by one domain.
 *
 * <p>The entity stores the local user name, password, display name and the
 * domain that owns the account. Together, {@code name} and {@code domain}
 * define the canonical address used by the rest of the project.
 */
@Entity
public class User {
    @Id
    private String name;
    private String pwd;
    private String domain;
    @Column(length = 1024)
    private String displayName;

    /**
     * Creates an empty user instance required by Hibernate and JSON deserializers.
     */
    public User() {
        this.pwd = null;
        this.name = null;
        this.domain = null;
        this.displayName = null;
    }

    /**
     * Creates an empty user instance required by Hibernate and JSON deserializers.
     *
     * @param name local user name
     * @param pwd user password
     * @param displayName user-facing display name
     * @param domain domain that owns the account
     */
    public User(String name, String pwd, String displayName, String domain) {
        this.pwd = pwd;
        this.name = name;
        this.domain = domain;
        this.displayName = displayName;
    }

    /**
     * Returns the local user name.
     *
     * @return local user name
     */
    public String getName() {
        return name;
    }

    /**
     * Updates the local user name.
     *
     * @param name new user name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the stored password.
     *
     * @return stored password
     */
    public String getPwd() {
        return pwd;
    }

    /**
     * Updates the stored password.
     *
     * @param pwd new password
     */
    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    /**
     * Returns the user's display name.
     *
     * @return display name
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Updates the user's display name.
     *
     * @param displayName new display name
     */
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Returns the domain that owns this account.
     *
     * @return account domain
     */
    public String getDomain() {
        return domain;
    }

    /**
     * Updates the account domain.
     *
     * @param domain new domain
     */
    public void setDomain(String domain) {
        this.domain = domain;
    }

    /**
     * Returns a textual representation useful for logs and debugging.
     *
     * @return string representation of this user.
     */
    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", pwd='" + pwd + '\'' +
                ", displayName='" + displayName + '\'' +
                ", domain='" + domain + '\'' +
                '}';
    }
}
