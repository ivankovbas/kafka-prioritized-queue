# Kafka Prioritized Queue

 * Start zookeeper in daemon mode
 
        bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
        
 * Start kafka server in daemon mode
 
        bin/kafka-server-start.sh -daemon config/server.properties
 
 * Create topic
 
        bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic priority_topic_2
        
 * Show list of topics
 
        bin/kafka-topics.sh --list --zookeeper localhost:2181
        
 * Delete topic
 
        bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic test
        
 * Start console producer
 
        bin/kafka-console-producer.sh --broker-list localhost:9092 --topic priority_topic_1
        
 * Start console consumer
 
        bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic priority_topic_1 --from-beginning