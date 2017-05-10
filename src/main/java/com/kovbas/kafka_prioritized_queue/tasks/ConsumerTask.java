package com.kovbas.kafka_prioritized_queue.tasks;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.*;


@Component
public class ConsumerTask implements ApplicationRunner {

    @Value("#{${kafka_topics}}")
    private List<String> topics;

    @Value("#{${kafka_consumers_properties}}")
    private List<Properties> consumersProperties;

    private final List<ConsumerThread> consumerThreads = new ArrayList<>();


    @Override
    @Async
    public void run(ApplicationArguments applicationArguments) throws Exception {

        // Assert that number of topics is equals to number of consumers
        // as we will create separate consumer for each topic
        assert topics.size() == consumersProperties.size();

        // Initialize and start threads
        {

            for (int i = 0; i < topics.size(); i++) {

                ConsumerThread consumerThread = new ConsumerThread(
                        topics.get(i),
                        consumersProperties.get(i)
                );

                consumerThread.start();

                // thread order represents priority
                consumerThreads.add(consumerThread);
            }
        }



        while (true) {

            Thread.sleep(10); // only for testing purposes

            for (ConsumerThread thread : consumerThreads) {

                ConsumerRecords<String, String> records = thread.getConsumerRecords();

                if (records != null && !records.isEmpty()) {

                    records.forEach(this::handleRecord);

                    thread.reset(); // tell to thread that data is handled

                    break; // read queue from the beginning if data is received to support priority
                }
            }

        }

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

class ConsumerThread extends Thread {

    private final String topic;

    private final Properties consumerProperties;

    private final Consumer<String, String> consumer;

    private ConsumerRecords<String, String> consumerRecords;


    ConsumerThread (String topic, Properties consumerProperties) {

        this.topic              = topic;
        this.consumerProperties = consumerProperties;
        this.consumer           = new KafkaConsumer<>(consumerProperties);

        consumer.subscribe(Collections.singletonList(topic));
    }


    /**
     * Sets consumerRecords to null, it allows to commit and load new data
     */
    synchronized void reset() {

        consumerRecords = null;
    }


    synchronized ConsumerRecords<String, String> getConsumerRecords() {

        return consumerRecords;
    }


    synchronized private void setConsumerRecords(ConsumerRecords<String, String> records) {

        consumerRecords = records;
    }


    @Override
    public void run() {

        try {

            long lastPollTime = 0;
            long pollInterval = Long.parseLong(consumerProperties.getProperty("max.poll.interval.ms")) / 2;

            while (true) {


                // Get data from server
                while (getConsumerRecords() == null) {

                    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                    lastPollTime = System.currentTimeMillis();

                    if (!records.isEmpty()) {

                        setConsumerRecords(records);

                        consumer.pause(consumer.assignment());
                    }
                }


                // wait until data is handled by main thread
                // we need it to retain consumer assigned to the partitions
                while (getConsumerRecords() != null) {

                    if (System.currentTimeMillis() - lastPollTime > pollInterval) {
                        consumer.poll(0L);
                        lastPollTime = System.currentTimeMillis();
                    }
                }

                // this part will be running only after data was handled
                // by colling reset method by main thread
                {
                    consumer.resume(consumer.paused());

                    consumer.commitSync();
                }
            }

        } catch (Exception e) {

            e.printStackTrace();
        } finally {

            consumer.close();
        }

    }

}