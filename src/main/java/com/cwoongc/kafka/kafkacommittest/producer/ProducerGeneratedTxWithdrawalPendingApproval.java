package com.cwoongc.kafka.kafkacommittest.producer;

import com.cwoongc.kafka.kafkacommittest.message.WithdrawalCreated;
import com.cwoongc.kafka.kafkacommittest.message.WithdrawalPendingApproval;
import com.cwoongc.kafka.kafkacommittest.util.TimeUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.sun.tools.javac.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.math.BigDecimal;
import java.util.Properties;

@Slf4j
public class ProducerGeneratedTxWithdrawalPendingApproval {

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new AfterburnerModule());
    }

    private final Properties secureProducerConfig;

    public ProducerGeneratedTxWithdrawalPendingApproval(Properties secureProducerConfig) {
        this.secureProducerConfig = secureProducerConfig;
    }


    public void start(String topic) throws JsonProcessingException {

        Producer<String, String> kafkaProducer = new KafkaProducer<>(secureProducerConfig);

        long currentEpochTime = TimeUtils.getCurrentEpochTime();

        WithdrawalPendingApproval withdrawalPendingApproval = WithdrawalPendingApproval.builder()
                .statusCode("1234")
                .requestId("2000444888589")
                .organizationId("orgO")
                .walletId("walletW")
                .coinCode("ETH")
                .build();

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String,String>(
                topic
                ,0
                ,null
                , objectMapper.writeValueAsString(withdrawalPendingApproval)
        );

        Headers headers = producerRecord.headers();
        headers.add("eliKafkaHeader", "{\"transactionType\":\"WITHDRAWAL\",\"transactionStatus\":\"PENDING_APPROVAL\"}".getBytes());

        try {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (metadata != null) {
                    log.info("[P] Topic: {}, Partition: {}, offset: {}",
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset()
                    );
                } else {
                    log.error(exception.getMessage(), exception);
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            kafkaProducer.close();
        }
    }


}