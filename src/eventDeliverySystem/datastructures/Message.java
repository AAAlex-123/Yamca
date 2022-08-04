package eventDeliverySystem.datastructures;

import java.io.Serializable;

/**
 * A wrapper holding an object and specifying its type using an enum used for
 * Internet transportation. Used to facilitate uniform communication between all
 * remote components of the system.
 *
 * @author Dimitris Tsirmpas
 */
public class Message implements Serializable {
	private static final long serialVersionUID = 1L;

	private final MessageType type;
	private final Object      value;

	/**
	 * Constructs a Message with the given {@link MessageType type} and value.
	 *
	 * @param type  the type of the message
	 * @param value the value of the message
	 */
	public Message(MessageType type, Object value) {
		this.type = type;
		this.value = value;
	}

	/**
	 * Returns this Message's type.
	 *
	 * @return the type
	 */
	public MessageType getType() {
		return type;
	}

	/**
	 * Returns this Message's value.
	 *
	 * @return the value
	 */
	public Object getValue() {
		return value;
	}

	@Override
	public String toString() {
		return String.format("Message [type=%s, value=%s]", type, value);
	}

	/**
	 * Specifies the purpose of a Message, thereby indirectly indicating the type of
	 * its value.
	 *
	 * @author Alex Mandelias
	 * @author Dimitris Tsirmpas
	 */
	public enum MessageType {

		/** Indicates the start of Packet transmission. The value is the Topic's name */
		DATA_PACKET_SEND,

		/** Requests the actual Broker CI for a Topic. The value is the Topic's name */
		BROKER_DISCOVERY,

		/** Initialises a Consumer connection. The value is a TopicToken */
		INITIALISE_CONSUMER,

		/** Requests the creation of a Topic. The value is the Topic's name */
		CREATE_TOPIC,
	}
}
