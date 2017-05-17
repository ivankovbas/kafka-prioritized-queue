package com.kovbas.kafka_prioritized_queue.priority_consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;


public class PriorityConsumerTest {

    Consumer<String, String> consumer;

    PriorityConsumer<String, String> spyPriorityConsumer;

    List<String> topics = new ArrayList<>(Arrays.asList(
            "topic_1",
            "topic_2"
    ));

    Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsByPartition = new HashMap<>();

    {
        recordsByPartition.put(
                new TopicPartition(topics.get(0), 0),
                Arrays.asList(
                        new ConsumerRecord<>(topics.get(0), 0, 29L, "key1", "value1"),
                        new ConsumerRecord<>(topics.get(0), 0, 30L, "key2", "value2")
                )
        );
        recordsByPartition.put(
                new TopicPartition(topics.get(0), 1),
                Arrays.asList(
                        new ConsumerRecord<>(topics.get(0), 1, 20L, "key1", "value1")
                )
        );
        recordsByPartition.put(
                new TopicPartition(topics.get(1), 0),
                Arrays.asList(
                        new ConsumerRecord<>(topics.get(1), 0, 70L, "key1", "value1")
                )
        );
    }


    @Before
    public void setup() {

        // mock consumer
        consumer = mock(Consumer.class);

        // create priority consumer spy
        spyPriorityConsumer   = spy(new PriorityConsumer<>(consumer, topics));
    }


    /**
     * Check if correct result is returned
     */
    @Test
    public void testPoll() {

        final String topic = topics.get(0);

        final ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsByPartition);

        doReturn(recordsByPartition.keySet()).when(spyPriorityConsumer).assignment();
        doReturn(topic).when(spyPriorityConsumer).findTopicToRead();
        doReturn(records).when(spyPriorityConsumer).pollFromTopic(topic);

        assertEquals(

                records,

                spyPriorityConsumer.poll(0L)
        );

        // Check if PriorityTopic::pollFromTopic is called
        verify(spyPriorityConsumer).pollFromTopic(topic);
    }


    /**
     * Check forcePartitionsAssignment is called if no partitions is assigned
     */
    @Test
    public void testPollWorksAsExpectedIfNoPartitionsIsAssigned() {

        doReturn(new HashSet<TopicPartition>()).when(spyPriorityConsumer).assignment();
        doNothing().when(spyPriorityConsumer).forcePartitionsAssignment();

        spyPriorityConsumer.poll(0L);

        // Check that PriorityConsumer::forcePartitionsAssignment is called
        verify(spyPriorityConsumer).forcePartitionsAssignment();
    }


    /**
     * Check empty result is returned if there is no available topic to read
     */
    @Test
    public void testPollWorksAsExpectedIfNoTopicsToRead() {

        doReturn(recordsByPartition.keySet()).when(spyPriorityConsumer).assignment();
        doReturn(null).when(spyPriorityConsumer).findTopicToRead();

        assertEquals(

                ConsumerRecords.empty(),

                spyPriorityConsumer.poll(0L)
        );

        verify(spyPriorityConsumer).idlePoll();
    }


    /**
     * Test that forcePartitionsAssignment works as expected
     */
    @Test
    public void testForcePartitionsAssignment() {

        // mock PriorityConsumer::seek method
        doNothing().when(spyPriorityConsumer).seek(any(TopicPartition.class), any(Long.class));

        // Check it works correctly when empty result is returned from poll
        {

            doReturn(ConsumerRecords.empty()).when(spyPriorityConsumer).doPollFromConsumer(any(Long.class));

            spyPriorityConsumer.forcePartitionsAssignment();

            verify(spyPriorityConsumer, never()).seek(any(TopicPartition.class), any(Long.class));
        }


        // Check it works correctly when not empty result is returned from poll
        {

            // mock Consumer::poll method to return appropriate data
            doReturn(new ConsumerRecords<>(recordsByPartition)).when(spyPriorityConsumer).doPollFromConsumer(any(Long.class));

            spyPriorityConsumer.forcePartitionsAssignment();

            // Check that seek was called for all partitions with appropriate params
            for (TopicPartition partition : recordsByPartition.keySet()) {

                verify(spyPriorityConsumer).seek(
                        partition,
                        recordsByPartition.get(partition).get(0).offset()
                );
            }

            // Check that seek was called only 'number of partitions' times
            verify(spyPriorityConsumer, times(recordsByPartition.values().size()))
                    .seek(any(TopicPartition.class), any(Long.class));
        }
    }


    @Test
    public void testAssignment() {

        Set<TopicPartition> partitions = recordsByPartition.keySet();

        // mock Consumer::assignment method to return partitions set
        doReturn(partitions).when(consumer).assignment();

        assertEquals(partitions, spyPriorityConsumer.assignment());

        // check if Consumer::assignment method was called
        verify(consumer).assignment();
    }


    @Test
    public void testPollFromTopic() {

        final String topic = topics.get(0);

        final ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsByPartition);

        doNothing().when(spyPriorityConsumer).pauseTopicsExcept(topic);
        doNothing().when(spyPriorityConsumer).resumeAllTopics();
        doReturn(records).when(spyPriorityConsumer).doPollFromConsumer(anyLong());

        assertEquals(records, spyPriorityConsumer.pollFromTopic(topic));

        verify(spyPriorityConsumer).pauseTopicsExcept(topic);
        verify(spyPriorityConsumer).resumeAllTopics();
        verify(spyPriorityConsumer).doPollFromConsumer(anyLong());
    }


    @Test
    public void testPauseAllTopics() {

        Set<TopicPartition> partitions = recordsByPartition.keySet();

        doNothing().when(spyPriorityConsumer).pause(partitions);
        doReturn(partitions).when(spyPriorityConsumer).assignment();

        spyPriorityConsumer.pauseAllTopics();

        verify(spyPriorityConsumer).pause(partitions);
    }


    @Test
    public void testDoPollFromConsumer() {

        long timeout = 20;

        final ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsByPartition);

        doReturn(records).when(consumer).poll(timeout);

        assertEquals(

                records,

                spyPriorityConsumer.doPollFromConsumer(timeout)
        );

        verify(consumer).poll(timeout);
    }
}
