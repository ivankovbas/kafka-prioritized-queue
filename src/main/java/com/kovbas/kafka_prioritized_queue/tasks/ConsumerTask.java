package com.kovbas.kafka_prioritized_queue.tasks;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Component
public class ConsumerTask implements ApplicationRunner {

    @Value("#{${kafka_topics}}")
    private List<String> topics;

    @Value("#{${kafka_consumers_properties}}")
    private List<Properties> consumersProperties;

    private final List<KafkaConsumer<String, String>> consumers = new ArrayList<>();

    private final List<Future<Pair<String, ConsumerRecords<String, String>>>> futures = new ArrayList<>();

    /**
     * The map saves relations between topics and Callable instances that poll new data for the given topic
     */
    private final Map<String, Callable<Pair<String, ConsumerRecords<String, String>>>> topicsCallables = new HashMap<>();


    @Override
    @Async
    public void run(ApplicationArguments applicationArguments) throws Exception {

        // Assert that number of topics is equals to number of consumers
        // as we will create separate consumer for each topic
        assert topics.size() == consumersProperties.size();

        ExecutorService executor;

        // Initialize consumers, threads and so on
        {

            // Create new executor with thread pool size equals number of topics
            // because each topic will be read in separate thread
            executor = Executors.newFixedThreadPool(topics.size());


            for (int i = 0; i < topics.size(); i++) {

                final String topic      = topics.get(i);
                final Properties props  = consumersProperties.get(i);

                // Initialize consumer and subscribe it to specific topic
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList(topic));

                // Add consumer to consumers list
                consumers.add(consumer);

                // Create callable instance for the given consumer to poll data from it
                Callable<Pair<String, ConsumerRecords<String, String>>> callable = () ->
                    new Pair<>(topic, consumer.poll(Long.MAX_VALUE));

                // Save relation between topic and callable instance that poll data from the topic
                topicsCallables.put(topic, callable);

                // Collect Future objects to have ability to iterate through them
                futures.add(executor.submit(callable));
            }
        }


        try {

            while (true) {

                Thread.sleep(10); // only for testing purposes

                // Iterate through futures list
                for (ListIterator<Future<Pair<String, ConsumerRecords<String, String>>>> i = futures.listIterator();
                        i.hasNext(); ) {

                    final Future<Pair<String, ConsumerRecords<String, String>>> future = i.next();

                    if (future.isDone()) {

                        final String topic                              = future.get().getFirst();
                        final ConsumerRecords<String, String> records   = future.get().getSecond();

                        // Pass callable to execution again and replace future object with new one
                        i.set(executor.submit(topicsCallables.get(topic)));

                        if (!records.isEmpty()) {

                            records.forEach(this::handleRecord);

                            // if records was received we should check queue form the beginning
                            break;
                        }
                    }
                }

            }

        } finally {

            consumers.forEach(KafkaConsumer::close);

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


/**
 * Simple class for wrapping two elements
 *
 * @param <F> Type of first element in pair
 * @param <S> Type of second element in pair
 */
class Pair<F, S> {

    private F first;
    private S second;

    Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    F getFirst() {
        return first;
    }

    S getSecond() {
        return second;
    }
}
