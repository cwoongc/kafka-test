package com.cwoongc.kafka.kafkacommittest.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerGeneratedTxCommitter {

    private final Properties  secureConsumerConfig;

    public ConsumerGeneratedTxCommitter(Properties consumerConfig) {
        this.secureConsumerConfig = consumerConfig;
    }

    public void start(TopicPartition topicPartition, Long commitUntil) {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(secureConsumerConfig);

//        kafkaConsumer.assign(Collections.singletonList(topicPartition));
        kafkaConsumer.subscribe(Collections.singletonList(topicPartition.topic()));

        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                long offset = consumerRecord.offset();

                log.info("[A] Topic: {}, Partitions: {}, Offset: {}, Key: {}, Value: {}, Last Committed Offset: {}",
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        offset,
                        consumerRecord.key(),
                        consumerRecord.value(),
                        kafkaConsumer.committed(topicPartition)
                );

                if(offset <= commitUntil) {
                    kafkaConsumer.commitSync(Collections.singletonMap(
                            topicPartition, new OffsetAndMetadata(offset + 1)
                    ));
                    log.info("[A} Committed offset : {}", offset + 1);

                }
            }
        }




    }

}
