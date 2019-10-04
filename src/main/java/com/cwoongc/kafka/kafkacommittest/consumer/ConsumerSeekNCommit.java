package com.cwoongc.kafka.kafkacommittest.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerSeekNCommit {

    private final Properties consumerConfig;

    public ConsumerSeekNCommit(Properties consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public void start(TopicPartition topicPartition, Long offsetToSeek) {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);

        List<TopicPartition> topicPartitions = Collections.singletonList(topicPartition);

        kafkaConsumer.assign(topicPartitions);

        if(offsetToSeek.compareTo(0L) == 0) {
            kafkaConsumer.seekToBeginning(topicPartitions);
        } else if (offsetToSeek.compareTo(-1L) == 0) {
            kafkaConsumer.seekToEnd(topicPartitions);
        } else {
            kafkaConsumer.seek(topicPartition, offsetToSeek);
        }

        Long nextRecordOffset = kafkaConsumer.position(topicPartition);
        kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(nextRecordOffset)));


        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                long offset = consumerRecord.offset();

                log.info("[seekNCommit] Topic: {}, Partitions: {}, Offset: {}, Key: {}, Value: {}, Last Committed Offset: {}",
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
