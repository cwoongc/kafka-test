package com.cwoongc.kafka.kafkacommittest.producer;

import com.cwoongc.kafka.kafkacommittest.message.DeployAccountCreated;
import com.cwoongc.kafka.kafkacommittest.message.WithdrawalCreated;
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
import java.util.ArrayList;
import java.util.Properties;

@Slf4j
public class ProducerGeneratedTxWithdrawalCreated {

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new AfterburnerModule());
    }

    private final Properties secureProducerConfig;

    public ProducerGeneratedTxWithdrawalCreated(Properties secureProducerConfig) {
        this.secureProducerConfig = secureProducerConfig;
    }


    public void start(String topic) throws JsonProcessingException {

        Producer<String, String> kafkaProducer = new KafkaProducer<>(secureProducerConfig);

        long currentEpochTime = TimeUtils.getCurrentEpochTime();

        WithdrawalCreated withdrawalCreated = WithdrawalCreated.builder()
                .statusCode("0000")
                .requestId("2000444888589")
                .coinCode("ETH")
                .transferAmount(new BigDecimal("12.345"))
                .from(List.of(
                        WithdrawalCreated.Withdrawal.builder()
                                .organizationId("orgO")
                                .walletId("walletW")
                                .address("addrA")
                                .amount(new BigDecimal("12.345"))
                                .index(0)
                                .build()
                ))
                .to(List.of(
                        WithdrawalCreated.Deposit.builder()
                                .address("addrD")
                                .amount(new BigDecimal("12.345"))
                                .index(0)
                                .tag("")
                        .build()
                ))
                .fee(WithdrawalCreated.Fee.builder()
                        .organizationId("orgO")
                        .walletId("walletW")
                        .address("addrF")
                        .estimatedAmount(new BigDecimal("3.456"))
                        .coinCode("ETH")
                        .build()
                ).build();

//        DeployAccountCreated deployAccountCreated = DeployAccountCreated.builder()
//                .statusCode("0000")
//                .requestId(Long.toString(currentEpochTime))
//                .coinCode("LN")
//                .organizationId("orgA")
//                .walletId("walletA")
//                .build();


        ProducerRecord<String, String> producerRecord = new ProducerRecord<String,String>(
                topic
                ,0
                ,null
                , objectMapper.writeValueAsString(withdrawalCreated)
        );

        Headers headers = producerRecord.headers();
        headers.add("eliKafkaHeader", "{\"transactionType\":\"WITHDRAWAL\",\"transactionStatus\":\"CREATED\"}".getBytes());

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