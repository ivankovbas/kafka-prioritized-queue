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

        consumer.subscribe(topics);
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
                pauseTopicsExcept(topic);
                resultRecords = super.poll(0L);

            } else {

                // If there isn't topic with unpolled messages then pause
                // all topics and call poll to not cause session timeout
                pauseAllTopics();
                super.poll(1000L); // TODO: allow to set this value from outside
            }

            resumeAllTopics();

            if (!resultRecords.isEmpty()) {
                return resultRecords;
            }

            long elapsed    = System.currentTimeMillis() - start;
            remaining       = timeout - elapsed;

        } while (remaining > 0);

        return resultRecords;
    }


    /**
     * Force partition assignment without changing current offsets
     */
    private void forcePartitionsAssignment() {

        // Call poll to force partition assignment
        ConsumerRecords<K, V> data = super.poll(0L);

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
    private void pauseAllTopics() {
        pause(assignment());
    }


    /**
     * Pause all topics partitions except the giving one
     *
     * @param topic
     */
    private void pauseTopicsExcept(String topic) {

        Set<TopicPartition> topicPartitions = assignment().stream()
                .filter(topicPartition -> !topicPartition.topic().equals(topic))
                .collect(Collectors.toSet());

        pause(topicPartitions);
    }


    /**
     * Resume partitions of all topics
     */
    private void resumeAllTopics() {

        resume(paused());
    }


    /**
     * Returns highest priority topic with available messages
     *
     * @return
     */
    private String findTopicToRead() {

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
    private Map<String, List<TopicPartition>> getPartitionsByTopicMap() {

        return assignment().stream()
                .collect(Collectors.groupingBy(TopicPartition::topic));
    }

}
