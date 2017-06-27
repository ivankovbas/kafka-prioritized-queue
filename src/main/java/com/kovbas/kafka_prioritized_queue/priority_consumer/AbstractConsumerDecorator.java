package com.kovbas.kafka_prioritized_queue.priority_consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


abstract class AbstractConsumerDecorator<K, V> implements Consumer<K, V> {

    Consumer<K, V> consumer;

    AbstractConsumerDecorator(Consumer<K, V> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Set<TopicPartition> assignment() {
        return consumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return consumer.subscription();
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener) {
        consumer.subscribe(pattern, consumerRebalanceListener);
    }

    @Override
    public void unsubscribe() {
        consumer.unsubscribe();
    }

    @Override
    public ConsumerRecords<K, V> poll(long l) {
        return consumer.poll(l);
    }

    @Override
    public void commitSync() {
        consumer.commitSync();
    }

    @Override
    public void commitAsync() {
        consumer.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) {
        consumer.commitAsync(offsetCommitCallback);
    }

    @Override
    public void seek(TopicPartition topicPartition, long l) {
        consumer.seek(topicPartition, l);
    }

    @Override
    public long position(TopicPartition topicPartition) {
        return consumer.position(topicPartition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition topicPartition) {
        return consumer.committed(topicPartition);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return consumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        return consumer.partitionsFor(s);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return consumer.listTopics();
    }

    @Override
    public Set<TopicPartition> paused() {
        return consumer.paused();
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        consumer.close();
    }

    @Override
    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection collection) {
        return consumer.endOffsets(collection);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection collection) {
        return consumer.beginningOffsets(collection);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map map) {
        return consumer.offsetsForTimes(map);
    }

    @Override
    public void resume(Collection collection) {
        consumer.resume(collection);
    }

    @Override
    public void pause(Collection collection) {
        consumer.pause(collection);
    }

    @Override
    public void seekToEnd(Collection collection) {
        consumer.seekToEnd(collection);
    }

    @Override
    public void seekToBeginning(Collection collection) {
        consumer.seekToBeginning(collection);
    }

    @Override
    public void commitAsync(Map map, OffsetCommitCallback offsetCommitCallback) {
        consumer.commitAsync(map, offsetCommitCallback);
    }

    @Override
    public void commitSync(Map map) {
        consumer.commitSync(map);
    }

    @Override
    public void assign(Collection collection) {
        consumer.assign(collection);
    }

    @Override
    public void subscribe(Collection collection, ConsumerRebalanceListener consumerRebalanceListener) {
        consumer.subscribe(collection, consumerRebalanceListener);
    }

    @Override
    public void subscribe(Collection collection) {
        consumer.subscribe(collection);
    }
}