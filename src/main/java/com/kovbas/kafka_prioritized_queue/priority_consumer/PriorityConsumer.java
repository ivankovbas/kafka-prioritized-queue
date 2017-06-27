package com.kovbas.kafka_prioritized_queue.priority_consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class PriorityConsumer<K, V> extends AbstractConsumerDecorator<K, V> {


    /**
     * List of topics to consume.
     * Order of topics in list represents topics priority.
     */
    List<String> topics;


    /**
     * Creates a PriorityConsumer with topics priority.
     *
     * @param consumer A Consumer Instance
     * @param topics Topics priority.
     */
    public PriorityConsumer(Consumer<K, V> consumer, List<String> topics) {

        super(consumer);

        this.topics = topics;

        subscribe(topics);
    }


    @Override
    public ConsumerRecords<K, V> poll(long timeout) {

        long start      = System.currentTimeMillis();
        long remaining  = timeout;

        ConsumerRecords<K, V> resultRecords = ConsumerRecords.empty();

        if (assignment().isEmpty()) {
            forcePartitionsAssignment();
        }

        do {

            // Find highest priority topic with available messages
            String topic = findTopicToRead();


            if (topic != null) {

                // poll data from specific topic and handle messages
                resultRecords = pollFromTopic(topic);

            } else {

                // If there isn't topic with unpolled messages then pause
                // all topics and call poll to not cause session timeout
                idlePoll();
            }


            if (!resultRecords.isEmpty()) {
                return resultRecords;
            }

            long elapsed    = System.currentTimeMillis() - start;
            remaining       = timeout - elapsed;

        } while (remaining > 0);

        return resultRecords;
    }


    /**
     * Polls data from the given topic
     * To poll data only from the given topic all partitions except partitions of the topic
     * will be paused. After making poll all partitions will be resumed.
     *
     * @param topic The name of topic to poll data from
     * @return New messages from the given topic
     */
    ConsumerRecords<K, V> pollFromTopic(String topic) {

        pauseTopicsExcept(topic);
        ConsumerRecords<K, V> resultRecords = doPollFromConsumer(0L);
        resumeAllTopics();

        return resultRecords;
    }


    /**
     * Method pauses all partitions and calls poll to retain connection live,
     * after that all partitions will be resumed
     */
    void idlePoll() {

        pauseAllTopics();
        doPollFromConsumer(1000L); // TODO: allow to set this value from outside
        resumeAllTopics();
    }


    /**
     * Force partition assignment without changing current offsets
     */
    void forcePartitionsAssignment() {

        // Call poll to force partition assignment
        ConsumerRecords<K, V> data = doPollFromConsumer(0L);


        // Reset current offset to previous position
        data.partitions().forEach(partition ->

                data.records(partition).stream()

                        .findFirst().ifPresent(record -> {

                            seek(partition, record.offset());

                        }
                )
        );
    }


    /**
     * Pause partitions of all topics
     */
    void pauseAllTopics() {
        pause(assignment());
    }


    /**
     * Pause all topics partitions except the giving one
     *
     * @param topic
     */
    void pauseTopicsExcept(String topic) {

        Set<TopicPartition> topicPartitions = assignment().stream()
                .filter(topicPartition -> !topicPartition.topic().equals(topic))
                .collect(Collectors.toSet());

        pause(topicPartitions);
    }


    /**
     * Resume partitions of all topics
     */
    void resumeAllTopics() {

        resume(paused());
    }


    /**
     * Returns highest priority topic with available messages
     *
     * @return
     */
    String findTopicToRead() {

        // Map topic partitions to topics
        Map<String, List<TopicPartition>> partitionsByTopic = getPartitionsByTopicMap();

        // Get end offsets of all partitions from the server
        Map<TopicPartition, Long> endOffsets = endOffsets(assignment());

        if (partitionsByTopic.isEmpty() || endOffsets.isEmpty()) {
            return null;
        }

        // Go through topics by priority and check if there is new messages for the topic
        for (String topic : topics) {

            for (TopicPartition partition : partitionsByTopic.get(topic)) {

                if (position(partition) < endOffsets.get(partition)) {

                    return topic;
                }

            }
        }

        return null;
    }


    /**
     * Map topic partitions to topics
     *
     * @return
     */
    Map<String, List<TopicPartition>> getPartitionsByTopicMap() {

        return assignment().stream()
                .collect(Collectors.groupingBy(TopicPartition::topic));
    }


    /**
     * Polls data from the origin Consumer instance
     *
     * @param timeout
     * @return
     */
    ConsumerRecords<K, V> doPollFromConsumer(long timeout) {

        return consumer.poll(timeout);
    }
}
