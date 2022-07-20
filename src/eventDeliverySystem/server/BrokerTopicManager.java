package eventDeliverySystem.server;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.dao.ITopicDAO;
import eventDeliverySystem.util.LG;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A thread-safe manager for BrokerTopics which is used by Brokers to manage their Topics.
 *
 * @author Alex Mandelias
 */
final class BrokerTopicManager implements AutoCloseable, Iterable<BrokerTopic> {

    private final ITopicDAO postDao;
    private final Map<String, Set<Socket>> consumerSocketsPerTopic = new HashMap<>();
    private final Map<String, BrokerTopic> topicsByName = new HashMap<>();

    /**
     * Constructs a manager for BrokerTopic objects.
     *
     * @param postDao the ITopicDAO object this manager uses to save the Topics
     *
     * @throws IOException if an I/O Error occurs while reading existing Topics from the ITopicDAO
     *                     object
     */
    BrokerTopicManager(ITopicDAO postDao) throws IOException {
        LG.sout("BrokerTopicManager(%s)", postDao);
        LG.in();
        this.postDao = postDao;
        synchronized (this.postDao) {
            for (AbstractTopic abstractTopic : this.postDao.readAllTopics()) {
                LG.sout("abstractTopic=%s", abstractTopic);
                addExistingTopic(new BrokerTopic(abstractTopic, this.postDao));
            }
        }
        LG.out();
    }

    @Override
    public void close() throws Exception {
        for (final Set<Socket> consumerSocketSet : consumerSocketsPerTopic.values())
            for (final Socket socket : consumerSocketSet) {
                socket.shutdownOutput();
                socket.close();
            }
    }

    @Override
    public Iterator<BrokerTopic> iterator() {
        return topicsByName.values().iterator();
    }

    /**
     * Returns whether a Topic with the given name exists in this manager.
     *
     * @param topicName the name of the Topic
     *
     * @return {@code true} if it exists, {@code false} otherwise
     */
    boolean topicExists(String topicName) {
        synchronized (topicsByName) {
            return topicsByName.containsKey(topicName);
        }
    }

    /**
     * Returns the BrokerTopic with the given name.
     *
     * @param topicName the name of the BrokerTopic
     *
     * @return the BrokerTopic with that name
     *
     * @throws NoSuchElementException if no BrokerTopic with that name exists in this manager
     */
    BrokerTopic getTopic(String topicName) throws NoSuchElementException {
        assertTopicExists(topicName);

        synchronized (topicsByName) {
           return topicsByName.get(topicName);
       }
    }

    /**
     * Creates a new BrokerTopic and adds it to this manager while also saving it to the ITopicDAO
     * object.
     *
     * @param topicName the name of the BrokerTopic to create
     *
     * @throws IOException if an I/O Error occurred while saving the BrokerTopic to the ITopicDAO
     * @throws IllegalArgumentException if a BrokerTopic with the given name already exists
     */
    void addTopic(String topicName) throws IOException, IllegalArgumentException {
        assertTopicDoesNotExist(topicName);

        BrokerTopic topic = new BrokerTopic(topicName, postDao);

        addExistingTopic(topic);

        synchronized (postDao) {
            postDao.createTopic(topicName);
        }
    }

    /**
     * Removes a BrokerTopic from this manager by closing all connections associated with it and
     * deleting it from the ITopicDAO object.
     *
     * @param topicName the name of the BrokerTopic to remove
     *
     * @throws IOException if an I/O Error occurred either while closing a connection associated
     *                     with it or while deleting it from the ITopicDAO object
     * @throws NoSuchElementException if no BrokerTopic with that name exists in this manager
     */
    void removeTopic(String topicName) throws IOException, NoSuchElementException {

        assertTopicExists(topicName);

        synchronized (topicsByName) {
            topicsByName.remove(topicName);
        }

        synchronized (consumerSocketsPerTopic) {
            for (final Socket socket : consumerSocketsPerTopic.get(topicName)) {
                socket.close();
            }

            consumerSocketsPerTopic.get(topicName).clear();
            consumerSocketsPerTopic.remove(topicName);
        }

        synchronized (postDao) {
            postDao.deleteTopic(topicName);
        }
    }

    /**
     * Registers a new connection for a BrokerTopic.
     *
     * @param topicName the name of the BrokerTopic to register the connection
     * @param socket the connection
     *
     * @throws NoSuchElementException if no BrokerTopic with that name exists in this manager
     */
    void registerConsumer(String topicName, Socket socket) throws NoSuchElementException {
        assertTopicExists(topicName);

        synchronized (consumerSocketsPerTopic) {
            consumerSocketsPerTopic.get(topicName).add(socket);
        }
    }

    private void addExistingTopic(BrokerTopic topic) throws IllegalArgumentException {

        String topicName = topic.getName();

        assertTopicDoesNotExist(topicName);

        synchronized (topicsByName) {
            topicsByName.put(topicName, topic);
        }

        synchronized (consumerSocketsPerTopic) {
            consumerSocketsPerTopic.put(topicName, new HashSet<>());
        }
    }

    private void assertTopicExists(String topicName) throws NoSuchElementException {
        if (!topicExists(topicName))
            throw new NoSuchElementException("No Topic with name '" + topicName + "' found");
    }

    private void assertTopicDoesNotExist(String topicName) throws IllegalArgumentException {
        if (topicExists(topicName))
            throw new IllegalArgumentException("Topic with name '" + topicName + "' already exists");
    }
}
