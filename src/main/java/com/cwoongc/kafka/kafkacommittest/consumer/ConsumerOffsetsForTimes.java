package com.cwoongc.kafka.kafkacommittest.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ConsumerOffsetsForTimes {

    private final Properties consumerConfig;

    public ConsumerOffsetsForTimes(Properties consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public void start(TopicPartition topicPartition, final long timestamp) {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);

        kafkaConsumer.assign(Collections.singletonList(topicPartition));


        final Map<TopicPartition, Long> timestampMapA = Collections.singletonMap(topicPartition, timestamp);
        final Map<TopicPartition, OffsetAndTimestamp> searchedOffset = kafkaConsumer.offsetsForTimes(timestampMapA);

        searchedOffset.forEach((k,v)->{
            log.info("[offsets-for-times] topic:{}, offset:{}, timestamp:{}",k.topic(), v.offset(), v.timestamp());
        });

        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                long offset = consumerRecord.offset();

                log.info("[offsets-for-times] Topic: {}, Partitions: {}, Offset: {}, Key: {}, Value: {}, Last Committed Offset: {}, Timestamp: {}",
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        offset,
                        consumerRecord.key(),
                        consumerRecord.value(),
                        kafkaConsumer.committed(topicPartition),
                        consumerRecord.timestamp()
                );

            }
        }

    }

}
