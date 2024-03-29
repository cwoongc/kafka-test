package com.cwoongc.kafka.kafkacommittest.consumer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerB {

    private final Properties consumerConfig;

    public ConsumerB(Properties consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public void start(TopicPartition topicPartition) {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);

        kafkaConsumer.subscribe(Collections.singletonList(topicPartition.topic()));

//        kafkaConsumer.assign(Collections.singletonList(topicPartition));

        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                long offset = consumerRecord.offset();

                log.info("[B] Topic: {}, Partitions: {}, Offset: {}, HeaderKey1: {}, header-eliKafkaHeader: {},  header-contentType: {}, header-spring_json_header_types: {}, Key: {}, Value: {}, Last Committed Offset: {}",
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        offset,
                        consumerRecord.headers().lastHeader("eliKafkaHeader").key(),
                        new String(consumerRecord.headers().lastHeader("eliKafkaHeader").value()),
                        new String(consumerRecord.headers().lastHeader("contentType").value()),
                        new String(consumerRecord.headers().lastHeader("spring_json_header_types").value()),
                        consumerRecord.key(),
                        consumerRecord.value(),
                        kafkaConsumer.committed(topicPartition)
                );

            }
        }




    }

}
