package com.kovbas.kafka_prioritized_queue.tasks;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.*;


@Component
public class ProducerTask implements ApplicationRunner {

    @Value("#{${kafka_topics}}")
    private List<String> topics;

    @Value("#{${kafka_producer_properties}}")
    private Properties props;


    /**
     * Method sends
     * 100 messages to high priority topic,
     * 200 messages to next priority topic
     * and so on...
     *
     * @param applicationArguments Application Arguments
     * @throws Exception
     */
    @Override
    @Async
    public void run(ApplicationArguments applicationArguments) throws Exception {

        List<String> topics = new ArrayList<>(this.topics);

        try (
                Producer<String, String> producer = new KafkaProducer<>(props)
        ) {

            int i = 0;

            while ( !topics.isEmpty() ) {

                i++;

                for(String topic : topics) {

                    producer.send(new ProducerRecord<>(

                            topic,

                            Integer.toString(i),

                            Integer.toString(i))

                    );

                }

                // remove high priority topic after each 100 messages
                if (i % 100 == 0) {
                    topics.remove(0);
                }

            }

        }

    }
}
