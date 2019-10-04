package com.cwoongc.kafka.kafkacommittest.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.stream.IntStream;

@Slf4j
public class ProducerP {

    private final Properties producerConfig;

    public ProducerP(Properties producerConfig) {
        this.producerConfig = producerConfig;
    }


    public void start(String topic, int producingSize) {

        Producer<String, String> kafkaProducer = new KafkaProducer<>(producerConfig);


        try {
            IntStream.rangeClosed(1, producingSize)
                    .forEach(no -> {

                        kafkaProducer.send(new ProducerRecord<String, String>(topic, no+""), (metadata, exception) -> {
                            if (metadata != null) {
                                log.info("[P] Topic: {}, Partition: {}, offset: {}, Producing No: {}",
                                        metadata.topic(),
                                        metadata.partition(),
                                        metadata.offset(),
                                        no
                                );
                            } else {
                                log.error(exception.getMessage(), exception);
                            }
                        });
                    });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            kafkaProducer.close();
        }

    }


}
