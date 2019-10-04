package com.cwoongc.kafka.kafkacommittest.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerNextRecord {

    private final Properties consumerConfig;

    public ConsumerNextRecord(Properties consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public void start(TopicPartition topicPartition) {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);

        List<TopicPartition> topicPartitions = Collections.singletonList(topicPartition);

        kafkaConsumer.assign(topicPartitions);

        Long nextRecordOffset = kafkaConsumer.position(topicPartition);
        log.info("[next-record] next record offset: {}", nextRecordOffset );


        kafkaConsumer.seek(topicPartition, nextRecordOffset);


        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                long offset = consumerRecord.offset();

                log.info("[next-record] Topic: {}, Partitions: {}, Offset: {}, Key: {}, Value: {}, Last Committed Offset: {}",
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        offset,
                        consumerRecord.key(),
                        consumerRecord.value(),
                        kafkaConsumer.committed(topicPartition)
                );

            }
        }




    }

}
