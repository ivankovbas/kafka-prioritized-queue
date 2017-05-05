package com.kovbas.kafka_prioritized_queue.tasks;

import com.sun.tools.javac.util.Assert;
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

    private final List<KafkaConsumer<String, String>> consumers = new ArrayList<>();


    @Override
    @Async
    public void run(ApplicationArguments applicationArguments) throws Exception {

        // Initialize consumers
        {
            for (int i = 0; i < consumersProperties.size(); i++) {

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumersProperties.get(i));
                consumer.subscribe(Collections.singletonList(topics.get(i)));

                consumers.add(consumer);

            }
        }

        try {

            while (true) {

                for (KafkaConsumer<String, String> consumer : consumers) {

                    ConsumerRecords<String, String> records = consumer.poll(0L);


                    if (!records.isEmpty()) {

                        records.forEach(this::handleRecord);

                        break;
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
