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

    private final Map<String, Callable<Pair<String, ConsumerRecords<String, String>>>> topicsCallables = new HashMap<>();


    @Override
    @Async
    public void run(ApplicationArguments applicationArguments) throws Exception {

        ExecutorService executor;

        // Initialize consumers, threads and so on
        {

            executor = Executors.newFixedThreadPool(consumersProperties.size());

            for (int i = 0; i < consumersProperties.size(); i++) {

                final String topic = topics.get(i);

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumersProperties.get(i));
                consumer.subscribe(Collections.singletonList(topic));

                consumers.add(consumer);

                Callable<Pair<String, ConsumerRecords<String, String>>> callable = () -> {
                    return new Pair<>(topic, consumer.poll(Long.MAX_VALUE));
                };

                futures.add(executor.submit(callable));
                topicsCallables.put(topics.get(i), callable);

            }
        }


        try {

            while (true) {

                Thread.sleep(10);

                for (ListIterator<Future<Pair<String, ConsumerRecords<String, String>>>> i = futures.listIterator();
                        i.hasNext(); ) {

                    final Future<Pair<String, ConsumerRecords<String, String>>> future = i.next();

                    if (future.isDone()) {

                        Pair<String, ConsumerRecords<String, String>> pair = future.get();

                        i.set(executor.submit(topicsCallables.get(pair.getFirst())));

                        if (!pair.getSecond().isEmpty()) {

                            pair.getSecond().forEach(this::handleRecord);

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

class Pair<F, S> {

    private F first;
    private S second;

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    public void setFirst(F first) {
        this.first = first;
    }

    public void setSecond(S second) {
        this.second = second;
    }

    public F getFirst() {
        return first;
    }

    public S getSecond() {
        return second;
    }
}
