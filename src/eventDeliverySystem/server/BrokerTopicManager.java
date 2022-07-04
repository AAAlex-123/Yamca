package eventDeliverySystem.server;

import eventDeliverySystem.filesystem.FileSystemException;
import eventDeliverySystem.filesystem.TopicFileSystem;

import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.*;

public class BrokerTopicManager implements AutoCloseable {

    private final TopicFileSystem tfs;
    private final Map<String, Set<ObjectOutputStream>> consumerOOSPerTopic = new HashMap<>();
    private final Map<String, BrokerTopic>             topicsByName = new HashMap<>();

    public BrokerTopicManager(Path topicsRootDirectory) throws FileSystemException {
        tfs = new TopicFileSystem(topicsRootDirectory);
    }

    @Override
    public void close() throws Exception {
        for (final Set<ObjectOutputStream> consumerOOSSet : consumerOOSPerTopic.values())
            for (final ObjectOutputStream consumerOOS : consumerOOSSet)
                consumerOOS.close();
    }

    public boolean topicExists(String topicName) {
        synchronized (topicsByName) {
            return topicsByName.containsKey(topicName);
        }
    }

    public BrokerTopic getTopic(String topicName) throws NoSuchElementException {
       if (!topicExists(topicName))
           throw new NoSuchElementException("There is no Topic with name " + topicName);

        synchronized (topicsByName) {
           return topicsByName.get(topicName);
       }
    }

    public void addTopic(String topicName) throws FileSystemException {
        BrokerTopic topic = new BrokerTopic(topicName, tfs);

        synchronized (topicsByName) {
            topicsByName.put(topicName, topic);
        }

        synchronized (consumerOOSPerTopic) {
            consumerOOSPerTopic.put(topicName, new HashSet<>());
        }

        synchronized (tfs) {
            tfs.createTopic(topicName);
        }
    }

    public void registerConsumer(String topicName, ObjectOutputStream oos) {
        synchronized (consumerOOSPerTopic) {
            consumerOOSPerTopic.get(topicName).add(oos);
        }
    }
}
