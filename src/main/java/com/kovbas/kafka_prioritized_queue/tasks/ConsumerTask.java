package com.kovbas.kafka_prioritized_queue.tasks;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;


@Component
public class ConsumerTask implements ApplicationRunner {

    @Value("#{${kafka_topics}}")
    private List<String> topics;

    @Value("#{${kafka_consumer_properties}}")
    private Properties consumerProperties;

    private Consumer<String, String> consumer;


    @Override
    @Async
    public void run(ApplicationArguments applicationArguments) throws Exception {

        consumer = new KafkaConsumer<>(consumerProperties);

        try {

            consumer.subscribe(topics);

            forcePartitionsAssignment();

            while (true) {

                // Find highest priority topic with available messages
                String topic = findTopicToRead();


                if (topic != null) {

                    // poll data from specific topic and handle messages
                    pauseTopicsExcept(topic);
                    consumer.poll(0L).forEach(this::handleRecord);

                } else {

                    // If there isn't topic with unpolled messages then pause
                    // all topics and call poll to not cause session timeout
                    pauseAllTopics();
                    consumer.poll(1000L);

                }

                resumeAllTopics();
            }

        } finally {
            consumer.close();
        }

    }


    /**
     * Force partition assignment without changing current offsets
     */
    private void forcePartitionsAssignment() {

        // Call poll to force partition assignment
        ConsumerRecords<String, String> data = consumer.poll(0L);

        // Reset current offset to previous position
        data.partitions().forEach(partition ->

                data.records(partition).stream()

                        .findFirst().ifPresent(record -> {

                            consumer.seek(partition, record.offset());

                        }
                )
        );
    }


    private void pauseAllTopics() {
        consumer.pause(consumer.assignment());
    }

    /**
     * Pause all topics partitions except the giving one
     *
     * @param topic
     */
    private void pauseTopicsExcept(String topic) {

        Set<TopicPartition> topicPartitions = consumer.assignment().stream()
                .filter(topicPartition -> !topicPartition.topic().equals(topic))
                .collect(Collectors.toSet());

        consumer.pause(topicPartitions);
    }


    /**
     * Resume partitions of all topics
     */
    private void resumeAllTopics() {

        consumer.resume(consumer.paused());
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
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());

        if (partitionsByTopic.isEmpty() || endOffsets.isEmpty()) {
            return null;
        }

        // Go through topics by priority and check if there is new messages for the topic
        for (String topic : topics) {

            for (TopicPartition partition : partitionsByTopic.get(topic)) {

                if (consumer.position(partition) < endOffsets.get(partition)) {

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

        return consumer.assignment().stream()
                .collect(Collectors.groupingBy(TopicPartition::topic));
    }


    private void handleRecord(ConsumerRecord<String, String> record) {

        System.out.printf(
                "topic = %s, offset = %d value = %s%n",
                record.topic(),
                record.offset(),
                record.value()
        );
    }
}
