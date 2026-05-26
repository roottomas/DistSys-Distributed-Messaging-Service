package sd2526.trab.api;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;

/**
 * Persistent representation of one message stored by the system.
 *
 * <p>A message may be stored in several inboxes, but it keeps a single global
 * identifier, sender description and creation timestamp. The destination set
 * records the intended recipients as addresses in the form {@code name@domain}.
 */
@Entity
public class Message {

	@Id
	private String id;
	private String sender;
	@ElementCollection(fetch = FetchType.EAGER)
	private Set<String> destination;
	private long creationTime;
	@Column(length = 1024)
	private String subject;
	@Lob
	@Column
	private String contents;
	
	/**
	 * Creates a message instance with the supplied fields.
	 */
	public Message() {
		this(null, null, Collections.emptySet(), null, null);
	}
	
	/**
	 * Creates a message instance with the supplied fields.
	 *
	 * @param sender sender representation, usually in address or display format
	 * @param destination destination address
	 * @param subject message subject
	 * @param contents message body
	 */
	public Message(String sender, String destination, String subject, String contents) {
		this(null, sender, Set.of(destination), subject, contents);
	}
	
	/**
	 * Creates a message instance with the supplied fields.
	 *
	 * @param sender sender representation, usually in address or display format
	 * @param destinations destination addresses
	 * @param subject message subject
	 * @param contents message body
	 */
	public Message(String sender, Set<String> destinations, String subject, String contents) {
		this(null, sender, destinations, subject, contents);
	}

	/**
	 * Creates a message instance with the supplied fields.
	 *
	 * @param id message identifier
	 * @param sender sender representation, usually in address or display format
	 * @param destination destination address
	 * @param subject message subject
	 * @param contents message body
	 */
	public Message(String id, String sender, String destination, String subject, String contents) {
		this(id, sender, Set.of(destination), subject, contents);
	}

	/**
	 * Creates a message instance with the supplied fields.
	 *
	 * @param id message identifier
	 * @param sender sender representation, usually in address or display format
	 * @param destinations destination addresses
	 * @param subject message subject
	 * @param contents message body
	 */
	public Message(String id, String sender, Set<String> destinations, String subject, String contents) {
		this.id = id;
		this.sender = sender;
		this.subject = subject;
		this.contents = contents;
		this.creationTime = System.currentTimeMillis();
		this.destination = new HashSet<String>(destinations);
	}

	/**
	 * Returns the sender string stored in the message.
	 *
	 * @return sender representation
	 */
	public String getSender() {
		return sender;
	}
	
	/**
	 * Updates the sender string stored in the message.
	 *
	 * @param sender new sender representation
	 */
	public void setSender(String sender) {
		this.sender = sender;
	}
	
	/**
	 * Returns the destination set associated with the message.
	 *
	 * @return destination addresses
	 */
	public Set<String> getDestination() {
		return destination;
	}
	
	/**
	 * Replaces the full destination set of the message.
	 *
	 * @param destination new destination addresses
	 */
	public void setDestination(Set<String> destination) {
		this.destination = destination;
	}
	
	/**
	 * Adds one recipient address to the destination set.
	 *
	 * @param destination destination address
	 */
	public void addDestination(String destination) {
		this.destination.add(destination);
	}

	/**
	 * Returns the creation timestamp of the message.
	 *
	 * @return creation timestamp in milliseconds
	 */
	public long getCreationTime() {
		return creationTime;
	}

	/**
	 * Updates the creation timestamp of the message.
	 *
	 * @param creationTime new creation timestamp in milliseconds
	 */
	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	/**
	 * Returns the message subject.
	 *
	 * @return subject text
	 */
	public String getSubject() {
		return subject;
	}

	/**
	 * Updates the message subject.
	 *
	 * @param subject new subject text
	 */
	public void setSubject(String subject) {
		this.subject = subject;
	}

	/**
	 * Returns the message body contents.
	 *
	 * @return message body
	 */
	public String getContents() {
		return contents;
	}

	/**
	 * Updates the message body contents.
	 *
	 * @param contents new message body
	 */
	public void setContents(String contents) {
		this.contents = contents;
	}

	/**
	 * Returns the global identifier of the message.
	 *
	 * @return message identifier
	 */
	public String getId() {
		return id;
	}

	/**
	 * Updates the global identifier of the message.
	 *
	 * @param id new message identifier
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Returns a textual representation useful for logs and debugging.
	 *
	 * @return string representation of this message.
	 */
	@Override
	public String toString() {
		return "Message{" +
				"id=" + id +
				", sender='" + sender + '\'' +
				", destination=" + destination +
				", creationTime=" + creationTime +
				", subject='" + subject + '\'' +
				", contents=" + (contents.length() > 20? contents.substring(0,20) : contents )+
				'}';
	}
}
