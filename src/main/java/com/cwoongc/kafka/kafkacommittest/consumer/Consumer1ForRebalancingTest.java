package com.cwoongc.kafka.kafkacommittest.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

@Slf4j
public class Consumer1ForRebalancingTest {

    private final Properties consumerConfig;

    public Consumer1ForRebalancingTest(Properties consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public void start(String topic) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);

        kafkaConsumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if(partitions.size() > 0) {
                    StringBuilder sb = new StringBuilder();
                    Iterator<TopicPartition> iterator =  partitions.iterator();
                    while(iterator.hasNext()) {
                        sb.append(iterator.next()).append(' ');
                    }
                    log.warn("[C1] onPartitionsRevoked: {}", sb);
                } else {
                    log.warn("[C1] onPartitionsRevoked: NOTHING");
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if(partitions.size() > 0) {
                    StringBuilder sb = new StringBuilder();
                    Iterator<TopicPartition> iterator =  partitions.iterator();
                    while(iterator.hasNext()) {
                        sb.append(iterator.next()).append(' ');
                    }
                    log.warn("[C1] onPartitionsAssigned: {}", sb);
                } else {
                    log.warn("[C1] onPartitionsAssigned: NOTHING");
                }
            }
        });

        while(true) {

            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            Set<TopicPartition> topicPartitions = kafkaConsumer.assignment();
            if(topicPartitions.size() == 0) {
                try {
                    Thread.sleep(1000);
                    continue;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            TopicPartition topicPartition = (TopicPartition) topicPartitions.toArray()[0];
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                long offset = consumerRecord.offset();

                log.info("[C1ForRebalancing] Topic: {}, Partitions: {}, Offset: {}, Key: {}, Value: {}, Last Committed Offset: {}",
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        offset,
                        consumerRecord.key(),
                        consumerRecord.value(),
                        kafkaConsumer.committed(topicPartition)
                );

                kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset)));

            }
        }
    }
}
