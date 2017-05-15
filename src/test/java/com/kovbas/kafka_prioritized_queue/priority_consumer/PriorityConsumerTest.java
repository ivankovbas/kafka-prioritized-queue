package com.kovbas.kafka_prioritized_queue.priority_consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.util.*;
import java.util.stream.Collectors;


import static org.mockito.Mockito.*;
import static org.junit.Assert.*;


public class PriorityConsumerTest {

    PriorityConsumer<String, String> priorityConsumer;

    List<String> topics = new ArrayList<>(Arrays.asList(
            "topic_1",
            "topic_2"
    ));

    // Will be used as cache by getConsumerRecordsForTopic method
    Map<String, ConsumerRecords<String, String>> consumerRecordsByTopics = new HashMap<>();

    Map<TopicPartition, List<ConsumerRecord<String, String>>> consumerRecordsByPartition = new HashMap<>();

    {
        consumerRecordsByPartition.put(

                new TopicPartition(topics.get(0), 0),

                Arrays.asList(
                        new ConsumerRecord<>(topics.get(0), 0, 1L, "key1", "value1")
                )
        );

        consumerRecordsByPartition.put(

                new TopicPartition(topics.get(1), 0),

                Arrays.asList(
                        new ConsumerRecord<>(topics.get(1), 0, 1L, "key2", "value2")
                )
        );
    }


    @Test
    public void testPollWorksAsExpected() {

        for (String topic : topics) {

            Consumer<String, String> consumer = mockConsumerToReadFromTopic(topic);

            priorityConsumer = new PriorityConsumer<>(consumer, topics);

            // Check that result is the same as expected
            assertEquals(

                    "Result should be received from the topic '" + topic + "'",

                    getConsumerRecordsForTopic(topic),

                    priorityConsumer.poll(0)
            );

            // Check that consumer was subscribed to all necessary topics
            verify(consumer).subscribe(ArgumentMatchers.eq(topics));

            // Check that all partitions except partitions of the given topic was paused
            verify(consumer).pause(
                    getPartitionsSetExceptTopic(topic)
            );
        }

    }


    private Consumer<String, String> mockConsumerToReadFromTopic(String topic) {

        Set<TopicPartition> partitionsSet = consumerRecordsByPartition.keySet();

        Consumer<String, String> consumer = mock(Consumer.class);

        when(consumer.assignment()).thenReturn(partitionsSet);

        when(consumer.endOffsets(partitionsSet)).thenReturn(getEndOffsets());

        // Mock consumer.position()
        {
            Map<TopicPartition, Long> currentOffsets = getCurrentOffsetsToReadFrom(topic);

            for (TopicPartition partition : currentOffsets.keySet()) {
                when(consumer.position(partition)).thenReturn(currentOffsets.get(partition));
            }
        }

        when(consumer.poll(0L)).thenReturn(getConsumerRecordsForTopic(topic));

        return consumer;

    }


    private Map<TopicPartition, Long> getCurrentOffsetsToReadFrom(String topicToRead) {

        Map<TopicPartition, Long> currentOffsets = new HashMap<>();

        int topicToReadIndex = topics.indexOf(topicToRead);

        for (int i = 0; i < topicToReadIndex; i++) {

            for (TopicPartition partition : getPartitionsForTopic(topics.get(i))) {

                List<ConsumerRecord<String, String>> records = consumerRecordsByPartition.get(partition);

                currentOffsets.put(

                        partition,

                        i < topicToReadIndex
                                ? records.get(records.size() - 1).offset() + 1
                                : 0
                );
            }

        }

        return currentOffsets;
    }


    private Map<TopicPartition, Long> getEndOffsets() {

        Map<TopicPartition, Long> endOffsets = new HashMap<>();

        for(TopicPartition partition : consumerRecordsByPartition.keySet()) {
            List<ConsumerRecord<String, String>> records = consumerRecordsByPartition.get(partition);
            endOffsets.put(partition, records.get(records.size() - 1).offset() + 1);
        }

        return endOffsets;
    }


    private ConsumerRecords<String, String> getConsumerRecordsForTopic(String topic) {

        if (consumerRecordsByTopics.containsKey(topic)) {
            return consumerRecordsByTopics.get(topic);
        }

        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();

        for(TopicPartition partition : consumerRecordsByPartition.keySet()) {

            if (!partition.topic().equals(topic)) {
                continue;
            }

            if (!records.containsKey(partition)) {
                records.put(partition, new ArrayList<>());
            }

            records.get(partition).addAll(consumerRecordsByPartition.get(partition));
        }

        ConsumerRecords<String, String> result = new ConsumerRecords<>(records);
        consumerRecordsByTopics.put(topic, result);

        return result;
    }


    private Set<TopicPartition> getPartitionsForTopic(String topic) {

        return consumerRecordsByPartition.keySet().stream()
                .filter(topicPartition -> topicPartition.topic().equals(topic))
                .collect(Collectors.toSet());
    }


    private Set<TopicPartition> getPartitionsSetExceptTopic(String topic) {

        return consumerRecordsByPartition.keySet().stream()
                .filter(topicPartition -> !topicPartition.topic().equals(topic))
                .collect(Collectors.toSet());
    }

}
