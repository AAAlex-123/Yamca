package eventDeliverySystem.server;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.ITopicDAO;
import eventDeliverySystem.util.LG;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

public class BrokerTopicManager implements AutoCloseable, Iterable<BrokerTopic> {

    private final ITopicDAO postDao;
    private final Map<String, Set<ObjectOutputStream>> consumerOOSPerTopic = new HashMap<>();
    private final Map<String, BrokerTopic>             topicsByName = new HashMap<>();

    public BrokerTopicManager(ITopicDAO postDao) throws IOException {
        LG.sout("BrokerTopicManager(%s)", postDao);
        LG.in();
        this.postDao = postDao;
        for (AbstractTopic abstractTopic : this.postDao.readAllTopics()) {
            LG.sout("abstractTopic=%s", abstractTopic);
            addExistingTopic(new BrokerTopic(abstractTopic, this.postDao));
        }
        LG.out();
    }

    @Override
    public void close() throws Exception {
        for (final Set<ObjectOutputStream> consumerOOSSet : consumerOOSPerTopic.values())
            for (final ObjectOutputStream consumerOOS : consumerOOSSet)
                consumerOOS.close();
    }

    @Override
    public Iterator<BrokerTopic> iterator() {
        return topicsByName.values().iterator();
    }

    public boolean topicExists(String topicName) {
        synchronized (topicsByName) {
            return topicsByName.containsKey(topicName);
        }
    }

    public BrokerTopic getTopic(String topicName) throws NoSuchElementException {
        assertTopicExists(topicName);

        synchronized (topicsByName) {
           return topicsByName.get(topicName);
       }
    }

    public void addTopic(String topicName) throws IOException, IllegalArgumentException {
        assertTopicDoesNotExist(topicName);

        BrokerTopic topic = new BrokerTopic(topicName, postDao);

        addExistingTopic(topic);

        synchronized (postDao) {
            postDao.createTopic(topicName);
        }
    }

    public void registerConsumer(String topicName, ObjectOutputStream oos) throws NoSuchElementException {
        assertTopicExists(topicName);

        synchronized (consumerOOSPerTopic) {
            consumerOOSPerTopic.get(topicName).add(oos);
        }
    }

    private void addExistingTopic(BrokerTopic topic) throws IllegalArgumentException {

        String topicName = topic.getName();

        assertTopicDoesNotExist(topicName);

        synchronized (topicsByName) {
            topicsByName.put(topicName, topic);
        }

        synchronized (consumerOOSPerTopic) {
            consumerOOSPerTopic.put(topicName, new HashSet<>());
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
