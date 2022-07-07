package eventDeliverySystem.server;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.filesystem.FileSystemException;
import eventDeliverySystem.filesystem.TopicFileSystem;
import eventDeliverySystem.util.LG;

import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.*;

public class BrokerTopicManager implements AutoCloseable, Iterable<BrokerTopic> {

    private final TopicFileSystem tfs;
    private final Map<String, Set<ObjectOutputStream>> consumerOOSPerTopic = new HashMap<>();
    private final Map<String, BrokerTopic>             topicsByName = new HashMap<>();

    public BrokerTopicManager(Path topicsRootDirectory) throws FileSystemException {
        LG.sout("BrokerTopicManager(%s)", topicsRootDirectory);
        LG.in();
        tfs = new TopicFileSystem(topicsRootDirectory);
        for (AbstractTopic abstractTopic : tfs.readAllTopics()) {
            LG.sout("abstractTopic=%s", abstractTopic);
            addExistingTopic(new BrokerTopic(abstractTopic, tfs));
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

    public void addTopic(String topicName) throws FileSystemException, IllegalArgumentException {
        assertTopicDoesNotExist(topicName);

        BrokerTopic topic = new BrokerTopic(topicName, tfs);

        addExistingTopic(topic);

        synchronized (tfs) {
            tfs.createTopic(topicName);
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
