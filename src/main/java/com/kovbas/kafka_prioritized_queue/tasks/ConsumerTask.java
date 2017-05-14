package com.kovbas.kafka_prioritized_queue.tasks;

import com.kovbas.kafka_prioritized_queue.priority_consumer.PriorityConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

    @Value("#{${kafka_consumer_properties}}")
    private Properties consumerProperties;


    @Override
    @Async
    public void run(ApplicationArguments applicationArguments) throws Exception {

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        consumer = new PriorityConsumer<>(consumer, topics);


        try {

            while (true) {

                consumer.poll(Long.MAX_VALUE).forEach(this::handleRecord);
            }

        } finally {

            consumer.close();
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
